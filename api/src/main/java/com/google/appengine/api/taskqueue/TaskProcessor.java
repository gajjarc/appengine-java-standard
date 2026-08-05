package com.google.appengine.api.taskqueue;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Transaction;
import com.google.apphosting.api.ApiProxy;
import com.google.cloud.tasks.v2beta3.AppEngineHttpRequest;
import com.google.cloud.tasks.v2beta3.AppEngineRouting;
import com.google.cloud.tasks.v2beta3.CloudTasksClient;
import com.google.cloud.tasks.v2beta3.CreateTaskRequest;
import com.google.cloud.tasks.v2beta3.QueueName;
import com.google.cloud.tasks.v2beta3.Task;
import com.google.cloud.tasks.v2beta3.TaskName;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Processor utility responsible for executing and dispatching pending Cloud Tasks stored in Datastore
 * ({@code _AE_PendingCloudTask}) to Google Cloud Tasks using the official Client SDK.
 *
 * <p>Handles task state transitions ({@code PENDING}, {@code PROCESSING}, {@code DONE}, {@code FAILED}),
 * exponential backoff retry tracking, GCP project and region discovery, and Client SDK task creation.
 */
public class TaskProcessor {
    private static final Logger logger = Logger.getLogger(TaskProcessor.class.getName());

    private static volatile CloudTasksClient sharedClient;

    private static CloudTasksClient getClient() {
        if (sharedClient == null) {
            synchronized (TaskProcessor.class) {
                if (sharedClient == null) {
                    try {
                        sharedClient = CloudTasksClient.create();
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to initialize CloudTasksClient in TaskProcessor", e);
                    }
                }
            }
        }
        return sharedClient;
    }

    /**
     * Processes a list of pending task entity IDs stored in Datastore.
     *
     * @param ids the list of Datastore entity IDs for {@code _AE_PendingCloudTask} entities to process
     */
    public static void processPendingTasks(List<Long> ids) {
        processPendingTasks(ids, false);
    }
    
    /**
     * Processes a list of pending task entity IDs stored in Datastore, indicating whether invocation
     * originated from the background sweeper cron job.
     *
     * @param ids the list of Datastore entity IDs for {@code _AE_PendingCloudTask} entities to process
     * @param handledBySweeper {@code true} if triggered by the cron sweeper; {@code false} if triggered by fast-path
     */
    public static void processPendingTasks(List<Long> ids, boolean handledBySweeper) {
        DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
        for (Long id : ids) {
            Key key = KeyFactory.createKey("_AE_PendingCloudTask", id);
            try {
                processSingleTask(ds, key, handledBySweeper);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Failed to process pending task " + id + ": " + e.getMessage(), e);
            }
        }
    }
    
    private static void processSingleTask(DatastoreService ds, Key key, boolean handledBySweeper) throws Exception {
        Transaction txn = ds.beginTransaction();
        Entity entity = null;
        try {
            entity = ds.get(txn, key);
            String status = (String) entity.getProperty("status");
            if ("DONE".equals(status) || "ALREADY_EXISTS".equals(status)) {
                txn.rollback();
                return;
            }
            
            entity.setProperty("status", "PROCESSING");
            entity.setProperty("lock_expires", new java.util.Date(System.currentTimeMillis() + 60000L));
            entity.setProperty("handled_by_sweeper", handledBySweeper);
            ds.put(txn, entity);
            txn.commit();
        } catch (Exception e) {
            if (txn.isActive()) {
                txn.rollback();
            }
            throw e;
        }
        
        String queueName = (String) entity.getProperty("queue_name");
        String payload = (String) entity.getProperty("cloud_task_payload");
        long entityId = key.getId();
        
        boolean success = false;
        com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueServiceError.ErrorCode resCode =
            com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueServiceError.ErrorCode.INTERNAL_ERROR;
        try {
            resCode = callCloudTasksViaSdk(queueName, payload, entityId, (String) entity.getProperty("cloud_task_name"));
            success = (resCode == com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueServiceError.ErrorCode.OK);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "CLOUDTASK: Exception during Client SDK dispatch for task " + entityId + ": " + ex.getMessage(), ex);
            success = false;
        }
        
        txn = ds.beginTransaction();
        try {
            entity = ds.get(txn, key);
            if (success) {
                entity.setProperty("status", "DONE");
                ds.delete(txn, key);
                logger.info("CLOUDTASK: Successfully processed and cleaned up task " + entityId);
            } else {
                Object retryObj = entity.getProperty("retry_count");
                long retryCount = (retryObj instanceof Number) ? ((Number) retryObj).longValue() : 0L;
                retryCount++;
                entity.setProperty("retry_count", retryCount);
                entity.setProperty("last_error", "Cloud Tasks Client SDK call failed with " + resCode);
                if (retryCount >= 5L) {
                    entity.setProperty("status", "FAILED");
                } else {
                    entity.setProperty("status", "PENDING");
                }
                entity.setProperty("lock_expires", null);
                ds.put(txn, entity);
                logger.warning("CLOUDTASK: Failed to process task " + entityId + ", retry count: " + retryCount);
            }
            txn.commit();
        } catch (Exception e) {
            if (txn.isActive()) {
                txn.rollback();
            }
            throw e;
        }
    }
    
    /**
     * Resolves the current Google Cloud Platform project ID from the App Engine runtime environment.
     *
     * @return the GCP project ID string
     */
    public static String getProjectId() {
        String appId = ApiProxy.getCurrentEnvironment().getAppId();
        if (appId != null && appId.contains("~")) {
            return appId.substring(appId.indexOf("~") + 1);
        }
        return appId;
    }

    /**
     * Resolves the current App Engine deployment location/region from environment variables, system properties,
     * or the GCP instance metadata server.
     *
     * @return the GCP region ID (e.g. {@code "us-central1"}, {@code "us-east1"})
     */
    public static String getLocation() {
        String zone = System.getenv("GAE_ZONE");
        if (zone != null && !zone.isEmpty()) {
            int lastDash = zone.lastIndexOf('-');
            if (lastDash > 0) {
                return zone.substring(0, lastDash);
            }
            return zone;
        }
        String location = System.getenv("LOCATION_ID");
        if (location != null && !location.isEmpty()) {
            return location;
        }
        location = System.getenv("GAE_LOCATION");
        if (location != null && !location.isEmpty()) {
            return location;
        }
        location = System.getenv("GAE_REGION");
        if (location != null && !location.isEmpty()) {
            return location;
        }
        location = System.getProperty("gae.location");
        if (location != null && !location.isEmpty()) {
            return location;
        }
        try {
            java.net.URL url = new java.net.URL("http://metadata.google.internal/computeMetadata/v1/instance/region");
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
            conn.setRequestProperty("Metadata-Flavor", "Google");
            conn.setConnectTimeout(2000);
            conn.setReadTimeout(2000);
            if (conn.getResponseCode() == 200) {
                try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(conn.getInputStream(), java.nio.charset.StandardCharsets.UTF_8))) {
                    String regionPath = reader.readLine();
                    if (regionPath != null) {
                        if (regionPath.contains("/")) {
                            return regionPath.substring(regionPath.lastIndexOf('/') + 1).trim();
                        }
                        return regionPath.trim();
                    }
                }
            }
        } catch (Exception e) {
            // Ignore metadata failure in local dev / testing
        }
        String localRegion = System.getenv("LOCAL_GCP_REGION");
        return (localRegion != null && !localRegion.isEmpty()) ? localRegion : "us-central1";
    }

    /**
     * Dispatches a single push task using the official Google Cloud Tasks Client SDK.
     *
     * @param queueName the target task queue name
     * @param payload the JSON task payload stored in Datastore
     * @param entityId the Datastore entity ID for fallback task naming
     * @param taskName the chosen task name or {@code null}
     * @return a {@link com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueServiceError.ErrorCode}
     *         indicating success or failure code
     */
    public static com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueServiceError.ErrorCode callCloudTasksViaSdk(
            String queueName, String payload, long entityId, String taskName) {
        String projectId = getProjectId();
        String location = getLocation();
        QueueName parent = QueueName.of(projectId, location, queueName);
        if (taskName == null || taskName.isEmpty()) {
            taskName = "task-" + entityId;
        }
        String fullTaskName = TaskName.of(projectId, location, queueName, taskName).toString();

        try {
            CloudTasksClient client = getClient();
            JsonObject json = new JsonParser().parse(payload).getAsJsonObject();
            JsonObject taskJson = json.getAsJsonObject("task");

            AppEngineHttpRequest.Builder appEngineHttpRequestBuilder = AppEngineHttpRequest.newBuilder();
            if (taskJson != null && taskJson.has("appEngineHttpRequest")) {
                JsonObject httpJson = taskJson.getAsJsonObject("appEngineHttpRequest");
                if (httpJson.has("relativeUri")) {
                    appEngineHttpRequestBuilder.setRelativeUri(httpJson.get("relativeUri").getAsString());
                }
                if (httpJson.has("body")) {
                    byte[] bodyBytes = Base64.getDecoder().decode(httpJson.get("body").getAsString());
                    appEngineHttpRequestBuilder.setBody(ByteString.copyFrom(bodyBytes));
                }
                if (httpJson.has("appEngineRouting")) {
                    JsonObject routingJson = httpJson.getAsJsonObject("appEngineRouting");
                    AppEngineRouting.Builder routingBuilder = AppEngineRouting.newBuilder();
                    if (routingJson.has("service")) {
                        routingBuilder.setService(routingJson.get("service").getAsString());
                    }
                    appEngineHttpRequestBuilder.setAppEngineRouting(routingBuilder.build());
                }
                if (httpJson.has("headers")) {
                    JsonObject headersJson = httpJson.getAsJsonObject("headers");
                    for (Map.Entry<String, JsonElement> entry : headersJson.entrySet()) {
                        appEngineHttpRequestBuilder.putHeaders(entry.getKey(), entry.getValue().getAsString());
                    }
                }
            }

            Task.Builder taskBuilder = Task.newBuilder()
                .setName(fullTaskName)
                .setAppEngineHttpRequest(appEngineHttpRequestBuilder.build());

            if (taskJson != null && taskJson.has("scheduleTime")) {
                String isoTime = taskJson.get("scheduleTime").getAsString();
                java.time.Instant instant = java.time.Instant.parse(isoTime);
                Timestamp ts = Timestamp.newBuilder()
                    .setSeconds(instant.getEpochSecond())
                    .setNanos(instant.getNano())
                    .build();
                taskBuilder.setScheduleTime(ts);
            }

            CreateTaskRequest request = CreateTaskRequest.newBuilder()
                .setParent(parent.toString())
                .setTask(taskBuilder.build())
                .build();

            client.createTask(request);
            return com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueServiceError.ErrorCode.OK;
        } catch (com.google.api.gax.rpc.AlreadyExistsException e) {
            logger.info("CLOUDTASK: Task already exists (idempotency): " + taskName);
            return com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueServiceError.ErrorCode.TASK_ALREADY_EXISTS;
        } catch (com.google.api.gax.rpc.NotFoundException e) {
            logger.warning("CLOUDTASK: Queue not found: " + queueName);
            return com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueServiceError.ErrorCode.UNKNOWN_QUEUE;
        } catch (Exception e) {
            logger.log(Level.SEVERE, "CLOUDTASK: Client SDK exception dispatching task " + taskName + ": " + e.getMessage(), e);
            return com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueServiceError.ErrorCode.INTERNAL_ERROR;
        }
    }
}
