package com.google.appengine.api.taskqueue;

import com.google.apphosting.api.ApiProxy;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.CompletableFuture;
import com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueAddRequest;
import com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueBulkAddRequest;
import com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueBulkAddResponse;
import com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueDeleteRequest;
import com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueDeleteResponse;
import com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueServiceError;
import com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueFetchQueueStatsRequest;
import com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueFetchQueueStatsResponse;
import com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueScannerQueueInfo;
import com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueuePurgeQueueRequest;
import com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueuePurgeQueueResponse;
import com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueMode;
import com.google.cloud.tasks.v2beta3.CloudTasksClient;
import com.google.cloud.tasks.v2beta3.GetQueueRequest;
import com.google.cloud.tasks.v2beta3.CreateTaskRequest;
import com.google.cloud.tasks.v2beta3.QueueName;
import com.google.cloud.tasks.v2beta3.Task;
import com.google.protobuf.ByteString;
import com.google.appengine.api.appidentity.AppIdentityService;
import com.google.appengine.api.appidentity.AppIdentityServiceFactory;
import java.util.Arrays;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import com.google.appengine.api.utils.FutureWrapper;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Transaction;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.datastore.Entity;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

public class InterceptorDelegate implements ApiProxy.Delegate<ApiProxy.Environment> {
    private static final Map<String, List<Long>> pendingTasksPerTxn = new java.util.concurrent.ConcurrentHashMap<>();
    private final ApiProxy.Delegate<ApiProxy.Environment> originalDelegate;

    @SuppressWarnings("unchecked")
    public InterceptorDelegate(ApiProxy.Delegate<?> originalDelegate) {
        this.originalDelegate = (ApiProxy.Delegate<ApiProxy.Environment>) originalDelegate;
    }

    private boolean isPullQueueRequest(String methodName, byte[] request) {
        try {
            if ("BulkAdd".equals(methodName)) {
                TaskQueueBulkAddRequest bulkRequest = TaskQueueBulkAddRequest.parseFrom(request);
                if (bulkRequest.getAddRequestCount() > 0) {
                    TaskQueueAddRequest req0 = bulkRequest.getAddRequest(0);
                    if (req0.getMode() == TaskQueueMode.Mode.PULL || (req0.hasQueueName() && req0.getQueueName().toStringUtf8().toLowerCase().contains("pull"))) {
                        return true;
                    }
                }
            } else if ("Delete".equals(methodName)) {
                TaskQueueDeleteRequest deleteRequest = TaskQueueDeleteRequest.parseFrom(request);
                if (deleteRequest.hasQueueName() && deleteRequest.getQueueName().toStringUtf8().toLowerCase().contains("pull")) {
                    return true;
                }
            } else if ("FetchQueueStats".equals(methodName)) {
                TaskQueueFetchQueueStatsRequest statsRequest = TaskQueueFetchQueueStatsRequest.parseFrom(request);
                if (statsRequest.getQueueNameCount() > 0 && statsRequest.getQueueName(0).toStringUtf8().toLowerCase().contains("pull")) {
                    return true;
                }
            } else if ("PurgeQueue".equals(methodName)) {
                TaskQueuePurgeQueueRequest purgeRequest = TaskQueuePurgeQueueRequest.parseFrom(request);
                if (purgeRequest.hasQueueName() && purgeRequest.getQueueName().toStringUtf8().toLowerCase().contains("pull")) {
                    return true;
                }
            }
        } catch (Exception e) {
            System.err.println("CLOUDTASK: Error checking pull queue request: " + e.getMessage());
        }
        return false;
    }

    @Override
    public byte[] makeSyncCall(ApiProxy.Environment environment, String packageName, String methodName, byte[] request) {
        System.out.println("*** CLOUDTASK CALL: " + packageName + "." + methodName + " ***");
        if ("taskqueue".equals(packageName) && ("BulkAdd".equals(methodName) || "Delete".equals(methodName) || "FetchQueueStats".equals(methodName) || "PurgeQueue".equals(methodName))) {
            String backend = System.getenv("GAE_PUSHQUEUE_BACKEND");
            if ("CLOUD_TASK".equals(backend)) {
                if (isPullQueueRequest(methodName, request)) {
                    return originalDelegate.makeSyncCall(environment, packageName, methodName, request);
                }
            }
        }
        if ("taskqueue".equals(packageName) && "BulkAdd".equals(methodName)) {
            String backend = System.getenv("GAE_PUSHQUEUE_BACKEND");
            if ("CLOUD_TASK".equals(backend)) {
                System.out.println("*** CLOUDTASK INTERCEPTED ***");
                try {
                    TaskQueueBulkAddRequest bulkRequest = TaskQueueBulkAddRequest.parseFrom(request);
                    TaskQueueBulkAddResponse.Builder responseBuilder = TaskQueueBulkAddResponse.newBuilder();
                    
                    String projectId = TaskProcessor.getProjectId();
                    String location = TaskProcessor.getLocation();
                    
                    AppIdentityService appIdentityService = AppIdentityServiceFactory.getAppIdentityService();
                    AppIdentityService.GetAccessTokenResult tokenResult = appIdentityService.getAccessToken(Arrays.asList("https://www.googleapis.com/auth/cloud-platform"));
                    String token = tokenResult.getAccessToken();
                    DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
                    Transaction txn = ds.getCurrentTransaction(null);
                    
                    List<String> taskNames = new ArrayList<>();
                    List<String> taskJsons = new ArrayList<>();
                    List<Entity> transactionalEntities = new ArrayList<>();
                    
                    String queueName = "";
                    if (bulkRequest.getAddRequestCount() > 0) {
                        queueName = bulkRequest.getAddRequest(0).getQueueName().toStringUtf8();
                    }
                    String fullQueueName = "projects/" + projectId + "/locations/" + location + "/queues/" + queueName;

                    for (int i = 0; i < bulkRequest.getAddRequestCount(); i++) {
                        TaskQueueAddRequest addRequest = bulkRequest.getAddRequest(i);
                        if (addRequest.hasTransaction()) {
                            String tId = Long.toString(addRequest.getTransaction().getHandle());
                            System.out.println("*** CLOUDTASK: Found txnId " + tId + " in BulkAdd ***");
                        }
                        String taskName = addRequest.getTaskName().toStringUtf8();
                        if (taskName == null || taskName.isEmpty() || "null".equals(taskName)) {
                            taskName = "task-" + java.util.UUID.randomUUID().toString();
                        }
                        taskNames.add(taskName);
                        
                        String taskJson = buildTaskJson(addRequest, fullQueueName, taskName, environment.getModuleId());
                        taskJsons.add(taskJson);
                        
                        if (txn != null) {
                            Entity pendingTask = new Entity("_AE_PendingCloudTask");
                            pendingTask.setProperty("queue_name", queueName);
                            pendingTask.setProperty("cloud_task_name", taskName);
                            pendingTask.setProperty("cloud_task_payload", taskJson);
                            pendingTask.setProperty("created", new java.util.Date());
                            pendingTask.setProperty("status", "PENDING");
                            pendingTask.setProperty("lock_expires", null);
                            pendingTask.setProperty("retry_count", 0L);
                            pendingTask.setProperty("last_error", "");
                            pendingTask.setProperty("handled_by_sweeper", false);
                            pendingTask.setProperty("sdk_lang", "JAVA");
                            transactionalEntities.add(pendingTask);
                        }
                    }
                    
                    if (txn != null) {
                        List<com.google.appengine.api.datastore.Key> keys = ds.put(txn, transactionalEntities);
                        String txnId = txn.getId();
                        List<Long> taskIds = pendingTasksPerTxn.computeIfAbsent(txnId, k -> new ArrayList<>());
                        for (com.google.appengine.api.datastore.Key key : keys) {
                            taskIds.add(key.getId());
                        }
                        
                        for (String taskName : taskNames) {
                            responseBuilder.addTaskResult(TaskQueueBulkAddResponse.TaskResult.newBuilder()
                                .setResult(TaskQueueServiceError.ErrorCode.OK)
                                .setChosenTaskName(ByteString.copyFromUtf8(taskName))
                                .build());
                        }
                    } else {
                        int chunkSize = 100;
                        for (int chunkStart = 0; chunkStart < taskJsons.size(); chunkStart += chunkSize) {
                            int chunkEnd = Math.min(chunkStart + chunkSize, taskJsons.size());
                            List<String> chunkJsons = taskJsons.subList(chunkStart, chunkEnd);
                            List<String> chunkNames = taskNames.subList(chunkStart, chunkEnd);

                            StringBuilder batchJsonBuilder = new StringBuilder();
                            batchJsonBuilder.append("{\"requests\": [");
                            for (int i = 0; i < chunkJsons.size(); i++) {
                                if (i > 0) batchJsonBuilder.append(",");
                                batchJsonBuilder.append("{");
                                batchJsonBuilder.append("\"parent\": \"").append(fullQueueName).append("\",");
                                batchJsonBuilder.append("\"task\": ").append(chunkJsons.get(i));
                                batchJsonBuilder.append("}");
                            }
                            batchJsonBuilder.append("]}");
                            String batchJson = batchJsonBuilder.toString();
                            
                            try {
                                java.net.URL url = new java.net.URL("https://cloudtasks.googleapis.com/v2beta3/" + fullQueueName + "/tasks:batchCreate");
                                java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
                                conn.setRequestMethod("POST");
                                conn.setRequestProperty("Authorization", "Bearer " + token);
                                conn.setRequestProperty("Content-Type", "application/json");
                                conn.setDoOutput(true);
                                
                                try (java.io.OutputStream os = conn.getOutputStream()) {
                                    byte[] input = batchJson.getBytes("utf-8");
                                    os.write(input, 0, input.length);
                                }
                                
                                int responseCode = conn.getResponseCode();
                                if (responseCode == 200 || responseCode == 201) {
                                    for (String taskName : chunkNames) {
                                        responseBuilder.addTaskResult(TaskQueueBulkAddResponse.TaskResult.newBuilder()
                                            .setResult(TaskQueueServiceError.ErrorCode.OK)
                                            .setChosenTaskName(ByteString.copyFromUtf8(taskName))
                                            .build());
                                    }
                                } else if (responseCode == 409) {
                                    System.err.println("CLOUDTASK: Batch create failed with 409 (Conflict) for chunk");
                                    for (String taskName : chunkNames) {
                                        responseBuilder.addTaskResult(TaskQueueBulkAddResponse.TaskResult.newBuilder()
                                            .setResult(TaskQueueServiceError.ErrorCode.TASK_ALREADY_EXISTS)
                                            .setChosenTaskName(ByteString.copyFromUtf8(taskName))
                                            .build());
                                    }
                                } else if (responseCode == 404) {
                                    System.err.println("CLOUDTASK: Batch create failed with 404 (Not Found) for chunk");
                                    for (String taskName : chunkNames) {
                                        responseBuilder.addTaskResult(TaskQueueBulkAddResponse.TaskResult.newBuilder()
                                            .setResult(TaskQueueServiceError.ErrorCode.UNKNOWN_QUEUE)
                                            .setChosenTaskName(ByteString.copyFromUtf8(taskName))
                                            .build());
                                    }
                                } else {
                                    String errorDetail = "";
                                    try (java.io.InputStream es = conn.getErrorStream()) {
                                        if (es != null) {
                                            java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(es, "utf-8"));
                                            StringBuilder sb = new StringBuilder();
                                            String line;
                                            while ((line = reader.readLine()) != null) {
                                                sb.append(line);
                                            }
                                            errorDetail = sb.toString();
                                        }
                                    } catch (Exception ex) {
                                        errorDetail = "Failed to read error stream: " + ex.getMessage();
                                    }
                                    System.err.println("CLOUDTASK: Batch create REST API failed with code " + responseCode + ", Detail: " + errorDetail);
                                    throw new RuntimeException("CLOUDTASK: Batch create REST API failed with code " + responseCode + ", Detail: " + errorDetail);
                                }
                            } catch (Exception e) {
                                System.err.println("CLOUDTASK: Failed to batch create tasks via REST: " + e.getMessage());
                                throw e;
                            }
                        }
                    }
                    return responseBuilder.build().toByteArray();
                } catch (Exception e) {
                    System.err.println("CLOUDTASK: Error diverting to Cloud Tasks: " + e.getMessage());
                    e.printStackTrace();
                    throw new RuntimeException("CLOUDTASK_DIVERSION_FAILED", e);
                }
            }
        } else if ("taskqueue".equals(packageName) && "Delete".equals(methodName)) {
            String backend = System.getenv("GAE_PUSHQUEUE_BACKEND");
            if ("CLOUD_TASK".equals(backend)) {
                System.out.println("*** CLOUDTASK INTERCEPTED DELETE ***");
                try {
                    TaskQueueDeleteRequest deleteRequest = TaskQueueDeleteRequest.parseFrom(request);
                    TaskQueueDeleteResponse.Builder responseBuilder = TaskQueueDeleteResponse.newBuilder();
                    
                    String projectId = TaskProcessor.getProjectId();
                    String location = TaskProcessor.getLocation();
                    String queueName = deleteRequest.getQueueName().toStringUtf8();
                    
                    com.google.appengine.api.appidentity.AppIdentityService appIdentityService = com.google.appengine.api.appidentity.AppIdentityServiceFactory.getAppIdentityService();
                    com.google.appengine.api.appidentity.AppIdentityService.GetAccessTokenResult tokenResult = appIdentityService.getAccessToken(java.util.Arrays.asList("https://www.googleapis.com/auth/cloud-platform"));
                    String token = tokenResult.getAccessToken();
                    
                    String fullQueueName = "projects/" + projectId + "/locations/" + location + "/queues/" + queueName;
                    
                    int chunkSize = 1000;
                    for (int chunkStart = 0; chunkStart < deleteRequest.getTaskNameCount(); chunkStart += chunkSize) {
                        int chunkEnd = Math.min(chunkStart + chunkSize, deleteRequest.getTaskNameCount());
                        
                        StringBuilder jsonBuilder = new StringBuilder();
                        jsonBuilder.append("{");
                        jsonBuilder.append("\"names\": [");
                        for (int i = chunkStart; i < chunkEnd; i++) {
                            String taskName = deleteRequest.getTaskName(i).toStringUtf8();
                            String fullTaskName = fullQueueName + "/tasks/" + taskName;
                            if (i > chunkStart) jsonBuilder.append(",");
                            jsonBuilder.append("\"").append(fullTaskName).append("\"");
                        }
                        jsonBuilder.append("]");
                        jsonBuilder.append("}");
                        
                        String json = jsonBuilder.toString();
                        
                        java.net.URL url = new java.net.URL("https://cloudtasks.googleapis.com/v2beta3/" + fullQueueName + "/tasks:batchDelete");
                        java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
                        conn.setRequestMethod("POST");
                        conn.setRequestProperty("Authorization", "Bearer " + token);
                        conn.setRequestProperty("Content-Type", "application/json");
                        conn.setDoOutput(true);
                        
                        try (java.io.OutputStream os = conn.getOutputStream()) {
                            byte[] input = json.getBytes("utf-8");
                            os.write(input, 0, input.length);
                        }
                        
                        int responseCode = conn.getResponseCode();
                        if (responseCode == 200 || responseCode == 202) {
                            StringBuilder responseContent = new StringBuilder();
                            try (java.io.BufferedReader in = new java.io.BufferedReader(new java.io.InputStreamReader(conn.getInputStream(), "utf-8"))) {
                                String inputLine;
                                while ((inputLine = in.readLine()) != null) {
                                    responseContent.append(inputLine);
                                }
                            }
                            String responseBody = responseContent.toString();
                            
                            TaskQueueServiceError.ErrorCode[] results = new TaskQueueServiceError.ErrorCode[chunkEnd - chunkStart];
                            java.util.Arrays.fill(results, TaskQueueServiceError.ErrorCode.OK);
                            
                            Pattern p = Pattern.compile("\"(\\d+)\"\\s*:\\s*\\{[^{}]*?\"code\"\\s*:\\s*(\\d+)");
                            Matcher m = p.matcher(responseBody);
                            while (m.find()) {
                                int idx = Integer.parseInt(m.group(1));
                                int code = Integer.parseInt(m.group(2));
                                if (idx >= 0 && idx < results.length) {
                                    switch (code) {
                                        case 5:
                                        case 404:
                                            results[idx] = TaskQueueServiceError.ErrorCode.UNKNOWN_TASK;
                                            break;
                                        case 3:
                                        case 400:
                                            results[idx] = TaskQueueServiceError.ErrorCode.INVALID_TASK_NAME;
                                            break;
                                        case 6:
                                        case 409:
                                            results[idx] = TaskQueueServiceError.ErrorCode.TASK_ALREADY_EXISTS;
                                            break;
                                        case 7:
                                        case 403:
                                            results[idx] = TaskQueueServiceError.ErrorCode.PERMISSION_DENIED;
                                            break;
                                        default:
                                            results[idx] = TaskQueueServiceError.ErrorCode.INTERNAL_ERROR;
                                            break;
                                    }
                                }
                            }
                            for (TaskQueueServiceError.ErrorCode res : results) {
                                responseBuilder.addResult(res);
                            }
                        } else {
                            System.err.println("CLOUDTASK: Batch delete failed with code " + responseCode);
                            throw new RuntimeException("CLOUDTASK: Batch delete failed with code " + responseCode);
                        }
                    }
                    return responseBuilder.build().toByteArray();
                } catch (Exception e) {
                    System.err.println("CLOUDTASK: Error diverting delete to Cloud Tasks: " + e.getMessage());
                    e.printStackTrace();
                    throw new RuntimeException("CLOUDTASK_DELETE_FAILED", e);
                }
            }
        } else if ("taskqueue".equals(packageName) && "FetchQueueStats".equals(methodName)) {
            String backend = System.getenv("GAE_PUSHQUEUE_BACKEND");
            if ("CLOUD_TASK".equals(backend)) {
                System.out.println("*** CLOUDTASK INTERCEPTED FETCH STATS ***");
                try {
                    TaskQueueFetchQueueStatsRequest statsRequest = TaskQueueFetchQueueStatsRequest.parseFrom(request);
                    String queueName = statsRequest.getQueueName(0).toStringUtf8();
                    
                    String projectId = TaskProcessor.getProjectId();
                    String location = TaskProcessor.getLocation();
                    String fullQueueName = "projects/" + projectId + "/locations/" + location + "/queues/" + queueName;
                    
                        AppIdentityService appIdentityService = AppIdentityServiceFactory.getAppIdentityService();
                        AppIdentityService.GetAccessTokenResult tokenResult = appIdentityService.getAccessToken(Arrays.asList("https://www.googleapis.com/auth/cloud-platform"));
                        String token = tokenResult.getAccessToken();
                        
                        String urlStr = "https://cloudtasks.googleapis.com/v2beta3/" + fullQueueName + "?readMask=stats";
                        java.net.URL url = new java.net.URL(urlStr);
                        java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
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
                            
                            // Extract stats using regex
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
                                String timeStr = m.group(1);
                                java.time.Instant instant = java.time.Instant.parse(timeStr);
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
                            
                            TaskQueueFetchQueueStatsResponse.Builder responseBuilder = TaskQueueFetchQueueStatsResponse.newBuilder();
                            TaskQueueFetchQueueStatsResponse.QueueStats.Builder legacyStatsBuilder = TaskQueueFetchQueueStatsResponse.QueueStats.newBuilder();
                            
                            legacyStatsBuilder.setNumTasks(tasksCount);
                            legacyStatsBuilder.setOldestEtaUsec(oldestEtaUsec);
                            
                            TaskQueueScannerQueueInfo.Builder scannerInfoBuilder = TaskQueueScannerQueueInfo.newBuilder();
                            scannerInfoBuilder.setExecutedLastMinute(executedLastMinute);
                            scannerInfoBuilder.setExecutedLastHour(0);
                            scannerInfoBuilder.setRequestsInFlight(requestsInFlight);
                            scannerInfoBuilder.setEnforcedRate(enforcedRate);
                            scannerInfoBuilder.setSamplingDurationSeconds(60.0); // Mocked to 60 seconds!
                            
                            legacyStatsBuilder.setScannerInfo(scannerInfoBuilder);
                            
                            responseBuilder.addQueueStats(legacyStatsBuilder);
                            
                            return responseBuilder.build().toByteArray();
                        } else {
                            throw new RuntimeException("CLOUDTASK: REST API failed with code " + responseCode);
                        }
                } catch (Exception e) {
                    System.err.println("CLOUDTASK: Failed to fetch stats: " + e.getMessage());
                    e.printStackTrace();
                    throw new RuntimeException("CLOUDTASK_STATS_FAILED", e);
                }
            }
        } else if ("taskqueue".equals(packageName) && "PurgeQueue".equals(methodName)) {
            String backend = System.getenv("GAE_PUSHQUEUE_BACKEND");
            if ("CLOUD_TASK".equals(backend)) {
                System.out.println("*** CLOUDTASK INTERCEPTED PURGE ***");
                try {
                    TaskQueuePurgeQueueRequest purgeRequest = TaskQueuePurgeQueueRequest.parseFrom(request);
                    String queueName = purgeRequest.getQueueName().toStringUtf8();
                    
                    String projectId = TaskProcessor.getProjectId();
                    String location = TaskProcessor.getLocation();
                    String fullQueueName = "projects/" + projectId + "/locations/" + location + "/queues/" + queueName;
                    
                    AppIdentityService appIdentityService = AppIdentityServiceFactory.getAppIdentityService();
                    AppIdentityService.GetAccessTokenResult tokenResult = appIdentityService.getAccessToken(Arrays.asList("https://www.googleapis.com/auth/cloud-platform"));
                    String token = tokenResult.getAccessToken();
                    
                    java.net.URL url = new java.net.URL("https://cloudtasks.googleapis.com/v2beta3/" + fullQueueName + ":purge");
                    java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
                    conn.setRequestMethod("POST");
                    conn.setRequestProperty("Authorization", "Bearer " + token);
                    conn.setRequestProperty("Content-Type", "application/json");
                    conn.setDoOutput(true);
                    
                    try (java.io.OutputStream os = conn.getOutputStream()) {
                        byte[] input = "{}".getBytes("utf-8");
                        os.write(input, 0, input.length);
                    }
                    
                    int responseCode = conn.getResponseCode();
                    if (responseCode == 200 || responseCode == 201) {
                        TaskQueuePurgeQueueResponse.Builder responseBuilder = TaskQueuePurgeQueueResponse.newBuilder();
                        return responseBuilder.build().toByteArray();
                    } else {
                        System.err.println("CLOUDTASK: Purge failed with code " + responseCode);
                        throw new RuntimeException("CLOUDTASK: Purge failed with code " + responseCode);
                    }
                } catch (Exception e) {
                    System.err.println("CLOUDTASK: Failed to purge queue: " + e.getMessage());
                    throw new RuntimeException("CLOUDTASK_PURGE_FAILED", e);
                }
            }
        }
        if ("datastore_v3".equals(packageName) && "Commit".equals(methodName)) {
            DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
            Transaction txn = ds.getCurrentTransaction(null);
            String txnId = null;
            if (txn != null) {
                txnId = txn.getId();
            }
            
            byte[] responseBytes = originalDelegate.makeSyncCall(environment, packageName, methodName, request);
            
            if (txnId != null) {
                Map<String, List<Long>> map = pendingTasksPerTxn;
                List<Long> taskIds = map.get(txnId);
                if (taskIds != null && !taskIds.isEmpty()) {
                    System.out.println("*** CLOUDTASK: Triggering Fast Path for txn " + txnId + " ***");
                    final List<Long> idsToProcess = new java.util.ArrayList<>(taskIds);
                    ApiProxy.Environment env = ApiProxy.getCurrentEnvironment();
                    CompletableFuture.runAsync(() -> {
                        ApiProxy.setEnvironmentForCurrentThread(env);
                        try {
                            System.out.println("*** CLOUDTASK: Fast Path processing for tasks: " + idsToProcess + " ***");
                            TaskProcessor.processPendingTasks(idsToProcess);
                        } finally {
                            ApiProxy.setEnvironmentForCurrentThread(null);
                        }
                    });
                    map.remove(txnId);
                }
            }
            return responseBytes;
        }
        
        return originalDelegate.makeSyncCall(environment, packageName, methodName, request);
    }

    @Override
    public Future<byte[]> makeAsyncCall(ApiProxy.Environment environment, String packageName, String methodName, byte[] request, ApiProxy.ApiConfig apiConfig) {
        System.out.println("*** CLOUDTASK ASYNC CALL: " + packageName + "." + methodName + " ***");
        if ("taskqueue".equals(packageName) && ("BulkAdd".equals(methodName) || "Delete".equals(methodName) || "FetchQueueStats".equals(methodName) || "PurgeQueue".equals(methodName))) {
            String backend = System.getenv("GAE_PUSHQUEUE_BACKEND");
            if ("CLOUD_TASK".equals(backend)) {
                if (isPullQueueRequest(methodName, request)) {
                    return originalDelegate.makeAsyncCall(environment, packageName, methodName, request, apiConfig);
                }
                if ("BulkAdd".equals(methodName)) {
                    try {
                        TaskQueueBulkAddRequest bulkRequest = TaskQueueBulkAddRequest.parseFrom(request);
                        boolean isTransactional = false;
                        for (TaskQueueAddRequest addReq : bulkRequest.getAddRequestList()) {
                            if (addReq.hasTransaction()) {
                                isTransactional = true;
                                break;
                            }
                        }
                        if (isTransactional) {
                            System.out.println("*** CLOUDTASK: Running BulkAdd synchronously for transactional task ***");
                            return java.util.concurrent.CompletableFuture.completedFuture(makeSyncCall(environment, packageName, methodName, request));
                        }
                    } catch (Exception e) {
                        System.out.println("*** CLOUDTASK: Failed to parse BulkAdd in makeAsyncCall: " + e.getMessage() + " ***");
                    }
                }
                
                ApiProxy.Environment env = ApiProxy.getCurrentEnvironment();
                return java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                    ApiProxy.setEnvironmentForCurrentThread(env);
                    try {
                        return makeSyncCall(environment, packageName, methodName, request);
                    } finally {
                        ApiProxy.setEnvironmentForCurrentThread(null);
                    }
                });
            }
        }
        if ("datastore_v3".equals(packageName) && "Commit".equals(methodName)) {
            String txnId = null;
            try {
                com.google.apphosting.datastore_bytes.proto2api.DatastoreV3Pb.Transaction txnProto = com.google.apphosting.datastore_bytes.proto2api.DatastoreV3Pb.Transaction.parseFrom(request);
                txnId = Long.toString(txnProto.getHandle());
                System.out.println("*** CLOUDTASK: Found txnId " + txnId + " in Commit ***");
            } catch (Exception e) {
                System.out.println("*** CLOUDTASK: Failed to parse Commit request: " + e.getMessage() + " ***");
            }
            
            Future<byte[]> future = originalDelegate.makeAsyncCall(environment, packageName, methodName, request, apiConfig);
            
            final String finalTxnId = txnId;
            return new FutureWrapper<byte[], byte[]>(future) {
                @Override
                protected byte[] wrap(byte[] responseBytes) throws Exception {
                    if (finalTxnId != null) {
                        Map<String, List<Long>> map = pendingTasksPerTxn;
                        List<Long> taskIds = map.get(finalTxnId);
                        if (taskIds != null && !taskIds.isEmpty()) {
                            System.out.println("*** CLOUDTASK: Triggering Fast Path for txn " + finalTxnId + " ***");
                            final List<Long> idsToProcess = new java.util.ArrayList<>(taskIds);
                            ApiProxy.Environment env = ApiProxy.getCurrentEnvironment();
                            CompletableFuture.runAsync(() -> {
                                ApiProxy.setEnvironmentForCurrentThread(env);
                                try {
                                    System.out.println("*** CLOUDTASK: Fast Path processing for tasks: " + idsToProcess + " ***");
                                    TaskProcessor.processPendingTasks(idsToProcess);
                                } finally {
                                    ApiProxy.setEnvironmentForCurrentThread(null);
                                }
                            });
                            map.remove(finalTxnId);
                        }
                    }
                    return responseBytes;
                }
                
                @Override
                protected Throwable convertException(Throwable cause) {
                    return cause;
                }
            };
        }
        
        return originalDelegate.makeAsyncCall(environment, packageName, methodName, request, apiConfig);
    }

    @Override
    public void log(ApiProxy.Environment environment, ApiProxy.LogRecord record) {
        originalDelegate.log(environment, record);
    }

    @Override
    public void flushLogs(ApiProxy.Environment environment) {
        originalDelegate.flushLogs(environment);
    }

    @Override
    public List<Thread> getRequestThreads(ApiProxy.Environment environment) {
        return originalDelegate.getRequestThreads(environment);
    }
    private static String buildTaskJson(TaskQueueAddRequest addRequest, String fullQueueName, String taskName, String serviceName) {
        String base64Body = java.util.Base64.getEncoder().encodeToString(addRequest.getBody().toByteArray());
        String relativeUrl = addRequest.getUrl().toStringUtf8();
        long etaUsec = addRequest.getEtaUsec();
        
        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{");
        jsonBuilder.append("\"name\": \"").append(fullQueueName).append("/tasks/").append(taskName).append("\",");
        jsonBuilder.append("\"appEngineHttpRequest\": {");
        jsonBuilder.append("\"appEngineRouting\": {");
        jsonBuilder.append("\"service\": \"").append(serviceName).append("\"");
        jsonBuilder.append("},");
        jsonBuilder.append("\"relativeUri\": \"").append(relativeUrl).append("\",");
        jsonBuilder.append("\"body\": \"").append(base64Body).append("\",");
        jsonBuilder.append("\"headers\": {");
        
        for (int j = 0; j < addRequest.getHeaderCount(); j++) {
            String key = addRequest.getHeader(j).getKey().toStringUtf8();
            String value = addRequest.getHeader(j).getValue().toStringUtf8();
            if (j > 0) jsonBuilder.append(",");
            jsonBuilder.append("\"").append(key).append("\": \"").append(value).append("\"");
        }
        
        jsonBuilder.append("}"); // end headers
        jsonBuilder.append("}"); // end appEngineHttpRequest
        
        if (etaUsec > 0) {
            String isoTime = java.time.Instant.ofEpochMilli(etaUsec / 1000).toString();
            jsonBuilder.append(",\"scheduleTime\": \"").append(isoTime).append("\"");
        }
        
        if (addRequest.hasRetryParameters()) {
            com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueRetryParameters retryParams = addRequest.getRetryParameters();
            jsonBuilder.append(",\"retryConfig\": {");
            boolean first = true;
            if (retryParams.hasRetryLimit()) {
                jsonBuilder.append("\"maxAttempts\": ").append(retryParams.getRetryLimit());
                first = false;
            }
            if (retryParams.hasAgeLimitSec()) {
                if (!first) jsonBuilder.append(",");
                jsonBuilder.append("\"maxRetryDuration\": \"").append(retryParams.getAgeLimitSec()).append("s\"");
                first = false;
            }
            if (retryParams.hasMinBackoffSec()) {
                if (!first) jsonBuilder.append(",");
                jsonBuilder.append("\"minBackoff\": \"").append(retryParams.getMinBackoffSec()).append("s\"");
                first = false;
            }
            if (retryParams.hasMaxBackoffSec()) {
                if (!first) jsonBuilder.append(",");
                jsonBuilder.append("\"maxBackoff\": \"").append(retryParams.getMaxBackoffSec()).append("s\"");
                first = false;
            }
            if (retryParams.hasMaxDoublings()) {
                if (!first) jsonBuilder.append(",");
                jsonBuilder.append("\"maxDoublings\": ").append(retryParams.getMaxDoublings());
                first = false;
            }
            jsonBuilder.append("}");
        }
        jsonBuilder.append("}");
        return jsonBuilder.toString();
    }
}
