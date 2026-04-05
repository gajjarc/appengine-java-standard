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

    @Override
    public byte[] makeSyncCall(ApiProxy.Environment environment, String packageName, String methodName, byte[] request) {
        System.out.println("*** CLOUDTASK CALL: " + packageName + "." + methodName + " ***");
        if ("taskqueue".equals(packageName) && "BulkAdd".equals(methodName)) {
            String backend = System.getenv("GAE_PUSHQUEUE_BACKEND");
            if ("CLOUD_TASK".equals(backend)) {
                System.out.println("*** CLOUDTASK INTERCEPTED ***");
                try {
                    TaskQueueBulkAddRequest bulkRequest = TaskQueueBulkAddRequest.parseFrom(request);
                    TaskQueueBulkAddResponse.Builder responseBuilder = TaskQueueBulkAddResponse.newBuilder();
                    
                    String projectId = "gae-direct-vpc";
                    String location = "us-east1";
                    
                    AppIdentityService appIdentityService = AppIdentityServiceFactory.getAppIdentityService();
                    AppIdentityService.GetAccessTokenResult tokenResult = appIdentityService.getAccessToken(Arrays.asList("https://www.googleapis.com/auth/cloud-platform"));
                    String token = tokenResult.getAccessToken();
                    DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
                    Transaction txn = ds.getCurrentTransaction(null);
                    
                    for (int i = 0; i < bulkRequest.getAddRequestCount(); i++) {
                        TaskQueueAddRequest addRequest = bulkRequest.getAddRequest(i);
                        if (addRequest.hasTransaction()) {
                            String tId = Long.toString(addRequest.getTransaction().getHandle());
                            System.out.println("*** CLOUDTASK: Found txnId " + tId + " in BulkAdd ***");
                        }
                        String queueName = addRequest.getQueueName().toStringUtf8();
                        String fullQueueName = "projects/" + projectId + "/locations/" + location + "/queues/" + queueName;
                        
                        String originalPayload = addRequest.getBody().toStringUtf8();
                        String base64Body = java.util.Base64.getEncoder().encodeToString(addRequest.getBody().toByteArray());
                        
                        String taskName = addRequest.getTaskName().toStringUtf8();
                        if (taskName == null || taskName.isEmpty() || "null".equals(taskName)) {
                            taskName = "task-" + java.util.UUID.randomUUID().toString();
                        }
                        
                        String relativeUrl = addRequest.getUrl().toStringUtf8();
                        long etaUsec = addRequest.getEtaUsec();
                        
                        StringBuilder jsonBuilder = new StringBuilder();
                        jsonBuilder.append("{");
                        jsonBuilder.append("\"task\": {");
                        jsonBuilder.append("\"name\": \"").append(fullQueueName).append("/tasks/").append(taskName).append("\",");
                        jsonBuilder.append("\"appEngineHttpRequest\": {");
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
                        
                        jsonBuilder.append("}"); // end task
                        jsonBuilder.append("}"); // end root
                        
                        String json = jsonBuilder.toString();
                        
                        if (txn != null) {
                            Entity pendingTask = new Entity("_AE_PendingCloudTask");
                            pendingTask.setProperty("queue_name", queueName);
                            pendingTask.setProperty("cloud_task_payload", json);
                            pendingTask.setProperty("created", new java.util.Date());
                            pendingTask.setProperty("status", "PENDING");
                            pendingTask.setProperty("sdk_lang", "JAVA");
                            
                            com.google.appengine.api.datastore.Key key = ds.put(txn, pendingTask);
                            
                            String txnId = txn.getId();
                            Map<String, List<Long>> map = pendingTasksPerTxn;
                            List<Long> taskIds = map.get(txnId);
                            if (taskIds == null) {
                                taskIds = new ArrayList<>();
                                map.put(txnId, taskIds);
                            }
                            taskIds.add(key.getId());
                            
                            responseBuilder.addTaskResult(TaskQueueBulkAddResponse.TaskResult.newBuilder()
                                .setResult(TaskQueueServiceError.ErrorCode.OK)
                                .setChosenTaskName(ByteString.copyFromUtf8(taskName))
                                .build());
                            continue;
                        }
                        
                        try {
                            java.net.URL url = new java.net.URL("https://cloudtasks.googleapis.com/v2beta3/" + fullQueueName + "/tasks");
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
                            if (responseCode == 200 || responseCode == 201) {
                                responseBuilder.addTaskResult(TaskQueueBulkAddResponse.TaskResult.newBuilder()
                                    .setResult(TaskQueueServiceError.ErrorCode.OK)
                                    .setChosenTaskName(ByteString.copyFromUtf8(taskName))
                                    .build());
                            } else if (responseCode == 409) {
                                System.err.println("CLOUDTASK: Task already exists: " + taskName);
                                responseBuilder.addTaskResult(TaskQueueBulkAddResponse.TaskResult.newBuilder()
                                    .setResult(TaskQueueServiceError.ErrorCode.TASK_ALREADY_EXISTS)
                                    .setChosenTaskName(ByteString.copyFromUtf8(taskName))
                                    .build());
                            } else {
                                System.err.println("CLOUDTASK: REST API failed with code " + responseCode);
                                throw new RuntimeException("CLOUDTASK: REST API failed with code " + responseCode);
                            }
                        } catch (Exception e) {
                            System.err.println("CLOUDTASK: Failed to create task via REST: " + e.getMessage());
                            throw e;
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
                    
                    String projectId = "gae-direct-vpc";
                    String location = "us-east1";
                    String queueName = deleteRequest.getQueueName().toStringUtf8();
                    
                    com.google.appengine.api.appidentity.AppIdentityService appIdentityService = com.google.appengine.api.appidentity.AppIdentityServiceFactory.getAppIdentityService();
                    com.google.appengine.api.appidentity.AppIdentityService.GetAccessTokenResult tokenResult = appIdentityService.getAccessToken(java.util.Arrays.asList("https://www.googleapis.com/auth/cloud-platform"));
                    String token = tokenResult.getAccessToken();
                    
                    String fullQueueName = "projects/" + projectId + "/locations/" + location + "/queues/" + queueName;
                    
                    StringBuilder jsonBuilder = new StringBuilder();
                    jsonBuilder.append("{");
                    jsonBuilder.append("\"names\": [");
                    for (int i = 0; i < deleteRequest.getTaskNameCount(); i++) {
                        String taskName = deleteRequest.getTaskName(i).toStringUtf8();
                        String fullTaskName = fullQueueName + "/tasks/" + taskName;
                        if (i > 0) jsonBuilder.append(",");
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
                    if (responseCode == 200 || responseCode == 202) { // 202 Accepted might be returned for Operations
                        for (int i = 0; i < deleteRequest.getTaskNameCount(); i++) {
                            responseBuilder.addResult(TaskQueueServiceError.ErrorCode.OK);
                        }
                    } else {
                        System.err.println("CLOUDTASK: Batch delete failed with code " + responseCode);
                        throw new RuntimeException("CLOUDTASK: Batch delete failed with code " + responseCode);
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
                    
                    String projectId = "gae-direct-vpc";
                    String location = "us-east1";
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
                    
                    String projectId = "gae-direct-vpc";
                    String location = "us-east1";
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
}
