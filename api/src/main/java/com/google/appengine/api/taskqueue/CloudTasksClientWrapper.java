package com.google.appengine.api.taskqueue;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.appidentity.AppIdentityService;
import com.google.appengine.api.appidentity.AppIdentityServiceFactory;
import com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueServiceError.ErrorCode;
import com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueFetchQueueStatsResponse;
import com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueScannerQueueInfo;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Clean Java client wrapper for Google Cloud Tasks API operations.
 * <p>
 * This class provides method-level integration between the legacy App Engine Task Queue API
 * ({@link QueueImpl}) and Google Cloud Tasks when the environment variable
 * {@code APPENGINE_USE_CLOUDTASK_PUSH_QUEUE} is set to {@code true}.
 */
public final class CloudTasksClientWrapper {

    private static final Logger logger = Logger.getLogger(CloudTasksClientWrapper.class.getName());
    private static final String ENV_VAR = "APPENGINE_USE_CLOUDTASK_PUSH_QUEUE";

    private CloudTasksClientWrapper() {}

    /**
     * Checks whether Cloud Tasks push queue routing is enabled via environment variable.
     *
     * @return {@code true} if Cloud Tasks push queue routing is enabled; {@code false} otherwise.
     */
    public static boolean isEnabled() {
        return Boolean.parseBoolean(System.getenv(ENV_VAR));
    }

    /**
     * Asynchronously enqueues one or more push tasks to Cloud Tasks or records them in Datastore if transactional.
     *
     * @param queueName the short name of the target App Engine queue
     * @param txn the active Datastore transaction, or {@code null} for non-transactional enqueue
     * @param taskOptionsList the list of task options to enqueue
     * @return a {@link Future} resolving to the list of created {@link TaskHandle}s
     */
    public static Future<List<TaskHandle>> addAsync(String queueName, Transaction txn, List<TaskOptions> taskOptionsList) {
        String effectiveQueue = (queueName == null || queueName.isEmpty()) ? "default" : queueName;
        String projectId = TaskProcessor.getProjectId();
        String location = TaskProcessor.getLocation();
        String fullQueueName = "projects/" + projectId + "/locations/" + location + "/queues/" + effectiveQueue;

        List<TaskHandle> createdHandles = new ArrayList<>();
        List<Entity> transactionalEntities = new ArrayList<>();
        List<String> transactionalTaskJsons = new ArrayList<>();
        List<String> transactionalTaskNames = new ArrayList<>();

        TaskAlreadyExistsException taee = null;

        for (TaskOptions options : taskOptionsList) {
            String taskName = options.getTaskName();
            if (taskName == null || taskName.isEmpty()) {
                taskName = "task-" + java.util.UUID.randomUUID().toString();
            }

            String jsonPayload = buildTaskJson(fullQueueName, taskName, options);

            long scheduleTimeMs = System.currentTimeMillis();
            if (options.getEtaMillis() != null) {
                scheduleTimeMs = options.getEtaMillis();
            } else if (options.getCountdownMillis() != null) {
                scheduleTimeMs += options.getCountdownMillis();
            }

            TaskOptions handleOptions = new TaskOptions(options);
            handleOptions.taskName(taskName);
            TaskHandle handle = new TaskHandle(handleOptions, effectiveQueue);
            handle.etaUsec(scheduleTimeMs * 1000L);

            if (txn != null) {
                Entity pendingTask = new Entity("_AE_PendingCloudTask");
                pendingTask.setProperty("queue_name", effectiveQueue);
                pendingTask.setProperty("cloud_task_name", taskName);
                pendingTask.setProperty("cloud_task_payload", jsonPayload);
                pendingTask.setProperty("created", new java.util.Date());
                pendingTask.setProperty("status", "PENDING");
                pendingTask.setProperty("lock_expires", null);
                pendingTask.setProperty("retry_count", 0L);
                pendingTask.setProperty("last_error", "");
                pendingTask.setProperty("handled_by_sweeper", false);
                pendingTask.setProperty("sdk_lang", "JAVA");
                transactionalEntities.add(pendingTask);
                transactionalTaskJsons.add(jsonPayload);
                transactionalTaskNames.add(taskName);
                createdHandles.add(handle);
            } else {
                ErrorCode code = TaskProcessor.callCloudTasks(effectiveQueue, jsonPayload, System.currentTimeMillis(), taskName);
                if (code == ErrorCode.OK) {
                    createdHandles.add(handle);
                } else if (code == ErrorCode.TASK_ALREADY_EXISTS) {
                    if (taee == null) {
                        taee = new TaskAlreadyExistsException("Task already exists: " + taskName);
                    }
                    taee.appendTaskName(taskName);
                } else if (code == ErrorCode.UNKNOWN_QUEUE) {
                    throw new IllegalStateException("The specified queue is unknown : " + effectiveQueue);
                } else {
                    throw new RuntimeException("Failed to enqueue task " + taskName + " to Cloud Tasks");
                }
            }
        }

        if (taee != null) {
            throw taee;
        }

        if (txn != null && !transactionalEntities.isEmpty()) {
            DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
            List<com.google.appengine.api.datastore.Key> keys = ds.put(txn, transactionalEntities);
            List<Long> taskIds = new ArrayList<>();
            for (com.google.appengine.api.datastore.Key k : keys) {
                taskIds.add(k.getId());
            }

            // Register in RequestCachingFilter for fast-path post-commit dispatch if available
            try {
                Class<?> filterClass = Class.forName("com.google.appengine.api.taskqueue.RequestCachingFilter");
                java.lang.reflect.Method addPendingMethod = filterClass.getMethod("addPendingTasks", List.class);
                addPendingMethod.invoke(null, taskIds);
            } catch (Exception e) {
                logger.log(Level.FINE, "RequestCachingFilter not present or reflective call failed", e);
            }
        }

        return CompletableFuture.completedFuture(createdHandles);
    }

    private static String buildTaskJson(String fullQueueName, String taskName, TaskOptions options) {
        String serviceName = System.getenv("GAE_SERVICE");
        if (serviceName == null || serviceName.isEmpty()) {
            serviceName = "taskqueue-java-cloudtask-service";
        }
        
        byte[] payload = options.getPayload();
        String base64Body = (payload != null && payload.length > 0) 
            ? java.util.Base64.getEncoder().encodeToString(payload) 
            : "";
            
        String relativeUrl = options.getUrl();
        if (relativeUrl == null || relativeUrl.isEmpty()) {
            relativeUrl = "/";
        }
        
        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{\"task\": {");
        jsonBuilder.append("\"name\": \"").append(fullQueueName).append("/tasks/").append(taskName).append("\",");
        jsonBuilder.append("\"appEngineHttpRequest\": {");
        jsonBuilder.append("\"appEngineRouting\": {");
        jsonBuilder.append("\"service\": \"").append(serviceName).append("\"");
        jsonBuilder.append("},");
        jsonBuilder.append("\"relativeUri\": \"").append(escapeJson(relativeUrl)).append("\",");
        if (!base64Body.isEmpty()) {
            jsonBuilder.append("\"body\": \"").append(base64Body).append("\",");
        }
        jsonBuilder.append("\"headers\": {");
        
        boolean firstHeader = true;
        for (Map.Entry<String, List<String>> entry : options.getHeaders().entrySet()) {
            for (String val : entry.getValue()) {
                if (!firstHeader) jsonBuilder.append(",");
                jsonBuilder.append("\"").append(escapeJson(entry.getKey())).append("\": \"")
                           .append(escapeJson(val)).append("\"");
                firstHeader = false;
            }
        }
        
        jsonBuilder.append("}"); // end headers
        jsonBuilder.append("}"); // end appEngineHttpRequest
        
        long etaMillis = System.currentTimeMillis();
        if (options.getEtaMillis() != null) {
            etaMillis = options.getEtaMillis();
        } else if (options.getCountdownMillis() != null) {
            etaMillis += options.getCountdownMillis();
        }
        if (etaMillis > System.currentTimeMillis() + 1000L) {
            String isoTime = java.time.Instant.ofEpochMilli(etaMillis).toString();
            jsonBuilder.append(",\"scheduleTime\": \"").append(isoTime).append("\"");
        }
        
        jsonBuilder.append("}}");
        return jsonBuilder.toString();
    }

    /**
     * Asynchronously deletes one or more tasks from Cloud Tasks by task handle.
     *
     * @param queueName the short name of the target App Engine queue
     * @param taskHandles the list of task handles to delete
     * @return a {@link Future} resolving to a list of booleans indicating deletion success
     */
    public static Future<List<Boolean>> deleteTaskAsync(String queueName, List<TaskHandle> taskHandles) {
        String effectiveQueue = (queueName == null || queueName.isEmpty()) ? "default" : queueName;
        String projectId = TaskProcessor.getProjectId();
        String location = TaskProcessor.getLocation();
        String fullQueueName = "projects/" + projectId + "/locations/" + location + "/queues/" + effectiveQueue;

        List<Boolean> results = new ArrayList<>();
        for (TaskHandle handle : taskHandles) {
            String fullTaskName = fullQueueName + "/tasks/" + handle.getName();
            try {
                TaskProcessor.deleteCloudTask(fullTaskName);
                results.add(Boolean.TRUE);
            } catch (Exception e) {
                logger.log(Level.WARNING, "Failed to delete Cloud Task: " + fullTaskName, e);
                results.add(Boolean.FALSE);
            }
        }
        return CompletableFuture.completedFuture(results);
    }

    /**
     * Asynchronously fetches statistics for the specified queue from Cloud Tasks.
     *
     * @param queueName the short name of the queue to fetch statistics for
     * @return a {@link Future} resolving to the {@link QueueStatistics}
     */
    public static Future<QueueStatistics> fetchStatisticsAsync(String queueName) {
        String effectiveQueue = (queueName == null || queueName.isEmpty()) ? "default" : queueName;
        String projectId = TaskProcessor.getProjectId();
        String location = TaskProcessor.getLocation();
        String fullQueueName = "projects/" + projectId + "/locations/" + location + "/queues/" + effectiveQueue;

        try {
            AppIdentityService appIdentityService = AppIdentityServiceFactory.getAppIdentityService();
            AppIdentityService.GetAccessTokenResult tokenResult = appIdentityService.getAccessToken(
                Collections.singletonList("https://www.googleapis.com/auth/cloud-platform")
            );
            String token = tokenResult.getAccessToken();

            String urlStr = "https://cloudtasks.googleapis.com/v2beta3/" + fullQueueName + "?readMask=stats";
            URL url = new URL(urlStr);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Authorization", "Bearer " + token);

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String inputLine;
                StringBuilder responseContent = new StringBuilder();
                while ((inputLine = in.readLine()) != null) {
                    responseContent.append(inputLine);
                }
                in.close();

                String json = responseContent.toString();
                int tasksCount = 0;
                long oldestEtaUsec = 0;
                int executedLastMinute = 0;
                int requestsInFlight = 0;
                double enforcedRate = 0.0;

                Pattern p = Pattern.compile("\"tasksCount\":\\s*\"(\\d+)\"");
                Matcher m = p.matcher(json);
                if (m.find()) tasksCount = Integer.parseInt(m.group(1));

                p = Pattern.compile("\"oldestEstimatedArrivalTime\":\\s*\"([^\"]+)\"");
                m = p.matcher(json);
                if (m.find()) {
                    Instant instant = Instant.parse(m.group(1));
                    oldestEtaUsec = instant.getEpochSecond() * 1000000L + instant.getNano() / 1000L;
                }

                p = Pattern.compile("\"executedLastMinuteCount\":\\s*\"(\\d+)\"");
                m = p.matcher(json);
                if (m.find()) executedLastMinute = Integer.parseInt(m.group(1));

                p = Pattern.compile("\"concurrentDispatchesCount\":\\s*\"(\\d+)\"");
                m = p.matcher(json);
                if (m.find()) requestsInFlight = Integer.parseInt(m.group(1));

                p = Pattern.compile("\"effectiveExecutionRate\":\\s*(\\d+(\\.\\d+)?)");
                m = p.matcher(json);
                if (m.find()) enforcedRate = Double.parseDouble(m.group(1));

                TaskQueueFetchQueueStatsResponse.QueueStats.Builder legacyStatsBuilder =
                    TaskQueueFetchQueueStatsResponse.QueueStats.newBuilder();
                legacyStatsBuilder.setNumTasks(tasksCount);
                legacyStatsBuilder.setOldestEtaUsec(oldestEtaUsec);

                TaskQueueScannerQueueInfo.Builder scannerInfoBuilder = TaskQueueScannerQueueInfo.newBuilder();
                scannerInfoBuilder.setExecutedLastMinute(executedLastMinute);
                scannerInfoBuilder.setExecutedLastHour(0);
                scannerInfoBuilder.setRequestsInFlight(requestsInFlight);
                scannerInfoBuilder.setEnforcedRate(enforcedRate);
                scannerInfoBuilder.setSamplingDurationSeconds(60.0);
                legacyStatsBuilder.setScannerInfo(scannerInfoBuilder);

                QueueStatistics stats = new QueueStatistics(effectiveQueue, legacyStatsBuilder.build());
                return CompletableFuture.completedFuture(stats);
            } else {
                throw new RuntimeException("CLOUDTASK: REST API failed with code " + responseCode);
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "CLOUDTASK: Failed to fetch stats for queue " + effectiveQueue + ": " + e.getMessage(), e);
            throw new RuntimeException("CLOUDTASK_STATS_FAILED", e);
        }
    }

    /**
     * Purges all tasks from the specified Cloud Tasks queue.
     *
     * @param queueName the short name of the target queue
     */
    public static void purge(String queueName) {
        String effectiveQueue = (queueName == null || queueName.isEmpty()) ? "default" : queueName;
        String projectId = TaskProcessor.getProjectId();
        String location = TaskProcessor.getLocation();
        String fullQueueName = "projects/" + projectId + "/locations/" + location + "/queues/" + effectiveQueue;

        try {
            AppIdentityService appIdentityService = AppIdentityServiceFactory.getAppIdentityService();
            AppIdentityService.GetAccessTokenResult tokenResult = appIdentityService.getAccessToken(
                Collections.singletonList("https://www.googleapis.com/auth/cloud-platform")
            );
            String token = tokenResult.getAccessToken();

            String urlStr = "https://cloudtasks.googleapis.com/v2beta3/" + fullQueueName + ":purge";
            URL url = new URL(urlStr);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Authorization", "Bearer " + token);
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            conn.getOutputStream().write("{}".getBytes(java.nio.charset.StandardCharsets.UTF_8));

            int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                throw new RuntimeException("CLOUDTASK: Purge queue failed with HTTP " + responseCode);
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "CLOUDTASK: Failed to purge queue " + effectiveQueue + ": " + e.getMessage(), e);
            throw new RuntimeException("CLOUDTASK_PURGE_FAILED", e);
        }
    }

    private static String escapeJson(String input) {
        if (input == null) return "";
        return input.replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r")
                    .replace("\t", "\\t");
    }
}
