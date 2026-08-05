package com.google.appengine.api.taskqueue;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Transaction;
import com.google.apphosting.api.ApiProxy;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Processor utility responsible for executing and dispatching pending Cloud Tasks stored in Datastore
 * ({@code _AE_PendingCloudTask}) to Google Cloud Tasks using the official Client SDK wrapper.
 *
 * <p>Handles task state transitions ({@code PENDING}, {@code PROCESSING}, {@code DONE}, {@code FAILED}),
 * exponential backoff retry tracking, GCP project and region discovery, and task dispatching via {@link CloudTasksClientWrapper}.
 */
public class TaskProcessor {
    private static final Logger logger = Logger.getLogger(TaskProcessor.class.getName());

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
     * Dispatches a single push task using {@link CloudTasksClientWrapper}.
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
        return CloudTasksClientWrapper.dispatchPendingTask(queueName, payload, entityId, taskName);
    }
}
