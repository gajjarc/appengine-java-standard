package com.google.appengine.api.taskqueue;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.appidentity.AppIdentityService;
import com.google.appengine.api.appidentity.AppIdentityServiceFactory;
import com.google.apphosting.api.ApiProxy;
import java.util.List;
import java.util.Arrays;
import java.util.Map;

public class TaskProcessor {
    public static void processPendingTasks(List<Long> ids) {
        processPendingTasks(ids, false);
    }
    
    public static void processPendingTasks(List<Long> ids, boolean handledBySweeper) {
        DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
        for (Long id : ids) {
            Key key = KeyFactory.createKey("_AE_PendingCloudTask", id);
            try {
                processSingleTask(ds, key, handledBySweeper);
            } catch (Exception e) {
                System.err.println("Failed to process pending task " + id + ": " + e.getMessage());
                e.printStackTrace();
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
        
        // Call Cloud Tasks (outside main transaction to avoid long locks)
        String queueName = (String) entity.getProperty("queue_name");
        String payload = (String) entity.getProperty("cloud_task_payload");
        long entityId = key.getId();
        
        boolean success = false;
        try {
            success = callCloudTasks(queueName, payload, entityId, (String) entity.getProperty("cloud_task_name"));
        } catch (Exception ex) {
            System.err.println("CLOUDTASK: Exception during REST dispatch for task " + entityId + ": " + ex.getMessage());
            success = false;
        }
        
        txn = ds.beginTransaction();
        try {
            entity = ds.get(txn, key);
            if (success) {
                entity.setProperty("status", "DONE");
                ds.delete(txn, key); // Cleanup on success
                System.out.println("CLOUDTASK: Successfully processed and cleaned up task " + entityId);
            } else {
                Object retryObj = entity.getProperty("retry_count");
                long retryCount = (retryObj instanceof Number) ? ((Number) retryObj).longValue() : 0L;
                retryCount++;
                entity.setProperty("retry_count", retryCount);
                entity.setProperty("last_error", "Cloud Tasks REST call failed");
                if (retryCount >= 5L) {
                    entity.setProperty("status", "FAILED");
                } else {
                    entity.setProperty("status", "PENDING"); // Revert to pending on failure
                }
                entity.setProperty("lock_expires", null);
                ds.put(txn, entity);
                System.err.println("CLOUDTASK: Failed to process task " + entityId + ", retry count: " + retryCount);
            }
            txn.commit();
        } catch (Exception e) {
            if (txn.isActive()) {
                txn.rollback();
            }
            throw e;
        }
    }
    
    public static String getProjectId() {
        String appId = ApiProxy.getCurrentEnvironment().getAppId();
        if (appId != null && appId.contains("~")) {
            return appId.substring(appId.indexOf("~") + 1);
        }
        return appId;
    }

    public static String getLocation() {
        String location = System.getenv("GAE_LOCATION");
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
            java.net.URL url = new java.net.URL("http://metadata.google.internal/computeMetadata/v1/instance/zone");
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
            conn.setRequestProperty("Metadata-Flavor", "Google");
            conn.setConnectTimeout(1000);
            conn.setReadTimeout(1000);
            if (conn.getResponseCode() == 200) {
                try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(conn.getInputStream(), java.nio.charset.StandardCharsets.UTF_8))) {
                    String zone = reader.readLine();
                    if (zone != null) {
                        if (zone.contains("/")) {
                            zone = zone.substring(zone.lastIndexOf('/') + 1);
                        }
                        int lastDash = zone.lastIndexOf('-');
                        if (lastDash > 0) {
                            return zone.substring(0, lastDash);
                        }
                        return zone;
                    }
                }
            }
        } catch (Exception e) {
            // Ignore metadata failure in local dev / testing
        }
        return "us-central1";
    }

    private static boolean callCloudTasks(String queueName, String payload, long entityId, String taskName) {
        String projectId = getProjectId();
        String location = getLocation();
        String fullQueueName = "projects/" + projectId + "/locations/" + location + "/queues/" + queueName;
        if (taskName == null || taskName.isEmpty()) {
            taskName = "task-" + entityId;
        }
        
        try {
            AppIdentityService appIdentityService = AppIdentityServiceFactory.getAppIdentityService();
            AppIdentityService.GetAccessTokenResult tokenResult = appIdentityService.getAccessToken(Arrays.asList("https://www.googleapis.com/auth/cloud-platform"));
            String token = tokenResult.getAccessToken();
            
            String updatedPayload = payload;
            try {
                com.google.gson.JsonObject json = new com.google.gson.JsonParser().parse(payload).getAsJsonObject();
                com.google.gson.JsonObject taskObj = json.getAsJsonObject("task");
                if (taskObj != null) {
                    taskObj.addProperty("name", fullQueueName + "/tasks/" + taskName);
                }
                updatedPayload = json.toString();
            } catch (Exception ex) {
                System.err.println("CLOUDTASK: Failed to parse payload JSON with Gson, falling back to string replacement: " + ex.getMessage());
                updatedPayload = payload.replaceAll("\"name\": \"[^\"]+\"", "\"name\": \"" + fullQueueName + "/tasks/" + taskName + "\"");
            }
            
            java.net.URL url = new java.net.URL("https://cloudtasks.googleapis.com/v2beta3/" + fullQueueName + "/tasks");
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Authorization", "Bearer " + token);
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            
            try (java.io.OutputStream os = conn.getOutputStream()) {
                byte[] input = updatedPayload.getBytes("utf-8");
                os.write(input, 0, input.length);
            }
            
            int responseCode = conn.getResponseCode();
            if (responseCode == 200 || responseCode == 201) {
                return true;
            } else if (responseCode == 409) {
                System.out.println("CLOUDTASK: Task already exists (idempotency): " + taskName);
                return true; // Treat as success
            } else {
                System.err.println("CLOUDTASK: Cloud Tasks call failed with code " + responseCode);
                return false;
            }
        } catch (Exception e) {
            System.err.println("CLOUDTASK: Exception calling Cloud Tasks: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
}
