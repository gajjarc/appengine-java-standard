package com.google.appengine.api.taskqueue;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.appidentity.AppIdentityService;
import com.google.appengine.api.appidentity.AppIdentityServiceFactory;
import java.util.List;
import java.util.Arrays;
import java.util.Map;

public class TaskProcessor {
    public static void processPendingTasks(List<Long> ids) {
        DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
        for (Long id : ids) {
            Key key = KeyFactory.createKey("_AE_PendingCloudTask", id);
            try {
                processSingleTask(ds, key);
            } catch (Exception e) {
                System.err.println("Failed to process pending task " + id + ": " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
    
    private static void processSingleTask(DatastoreService ds, Key key) throws Exception {
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
        
        boolean success = callCloudTasks(queueName, payload, entityId);
        
        txn = ds.beginTransaction();
        try {
            entity = ds.get(txn, key);
            if (success) {
                entity.setProperty("status", "DONE");
                ds.delete(txn, key); // Cleanup on success
                System.out.println("CLOUDTASK: Successfully processed and cleaned up task " + entityId);
            } else {
                entity.setProperty("status", "PENDING"); // Revert to pending on failure
                ds.put(txn, entity);
                System.err.println("CLOUDTASK: Failed to process task " + entityId + ", reverted to PENDING");
            }
            txn.commit();
        } catch (Exception e) {
            if (txn.isActive()) {
                txn.rollback();
            }
            throw e;
        }
    }
    
    private static boolean callCloudTasks(String queueName, String payload, long entityId) {
        String projectId = "gae-direct-vpc";
        String location = "us-east1";
        String fullQueueName = "projects/" + projectId + "/locations/" + location + "/queues/" + queueName;
        String taskName = "task-" + entityId;
        
        try {
            AppIdentityService appIdentityService = AppIdentityServiceFactory.getAppIdentityService();
            AppIdentityService.GetAccessTokenResult tokenResult = appIdentityService.getAccessToken(Arrays.asList("https://www.googleapis.com/auth/cloud-platform"));
            String token = tokenResult.getAccessToken();
            
            // The payload is already a JSON string representing the CreateTaskRequest!
            // But we need to update the task name to use our entityId!
            // The payload looks like: {"task": {"name": ".../tasks/task-UUID", "appEngineHttpRequest": {...}}}
            // We need to replace the task-UUID with task-entityId!
            
            String updatedPayload = payload.replaceAll("\"name\": \"[^\"]+\"", "\"name\": \"" + fullQueueName + "/tasks/" + taskName + "\"");
            
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
