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
import com.google.appengine.api.datastore.Entity;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Custom App Engine {@link ApiProxy.Delegate} that intercepts {@code taskqueue} RPC calls (such as
 * {@code BulkAdd}, {@code Delete}, {@code FetchQueueStats}, and {@code PurgeQueue}) and transparently
 * routes push queue operations to Google Cloud Tasks via the REST/Client SDK while delegating Datastore
 * transaction lifecycle calls and pull queue calls to the underlying delegate.
 */
public class InterceptorDelegate implements ApiProxy.Delegate<ApiProxy.Environment> {
    private static final Logger logger = Logger.getLogger(InterceptorDelegate.class.getName());
    private static final Map<String, List<Long>> pendingTasksPerTxn = new java.util.concurrent.ConcurrentHashMap<>();
    private final ApiProxy.Delegate<ApiProxy.Environment> originalDelegate;

    /**
     * Constructs a new {@code InterceptorDelegate} wrapping the specified original API proxy delegate.
     *
     * @param originalDelegate the underlying {@link ApiProxy.Delegate} to delegate unhandled or non-taskqueue API calls to
     */
    @SuppressWarnings("unchecked")
    public InterceptorDelegate(ApiProxy.Delegate<?> originalDelegate) {
        this.originalDelegate = (ApiProxy.Delegate<ApiProxy.Environment>) originalDelegate;
    }

    private static boolean isCloudTaskBackend() {
        return Boolean.parseBoolean(System.getenv("APPENGINE_USE_CLOUDTASK_PUSH_QUEUE"));
    }

    /**
     * Intercepts synchronous API proxy calls, routing push queue operations (such as {@code BulkAdd})
     * to Cloud Tasks while passing pull queues and other services to the original delegate.
     *
     * @param environment the current App Engine API execution environment
     * @param packageName the API service package name (e.g. {@code "taskqueue"})
     * @param methodName the API service method name (e.g. {@code "BulkAdd"})
     * @param request the serialized request protocol buffer bytes
     * @return the serialized response protocol buffer bytes
     */
    @Override
    public byte[] makeSyncCall(ApiProxy.Environment environment, String packageName, String methodName, byte[] request) {
        logger.fine("*** CLOUDTASK CALL: " + packageName + "." + methodName + " ***");
        if ("taskqueue".equals(packageName) && "BulkAdd".equals(methodName)) {
            if (isCloudTaskBackend()) {
                try {
                    TaskQueueBulkAddRequest bulkRequest = TaskQueueBulkAddRequest.parseFrom(request);
                    if (bulkRequest.getAddRequestCount() > 0 && bulkRequest.getAddRequest(0).getMode() == TaskQueueMode.Mode.PULL) {
                        return originalDelegate.makeSyncCall(environment, packageName, methodName, request);
                    }
                    logger.info("*** CLOUDTASK INTERCEPTED ***");
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
                    if (queueName == null || queueName.isEmpty()) {
                        queueName = "default";
                    }
                    String fullQueueName = "projects/" + projectId + "/locations/" + location + "/queues/" + queueName;

                    for (int i = 0; i < bulkRequest.getAddRequestCount(); i++) {
                        TaskQueueAddRequest addRequest = bulkRequest.getAddRequest(i);
                        if (addRequest.hasTransaction()) {
                            String tId = Long.toString(addRequest.getTransaction().getHandle());
                            logger.info("*** CLOUDTASK: Found txnId " + tId + " in BulkAdd ***");
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
                        String serviceName = environment.getModuleId();
                        int chunkSize = 100;
                        for (int chunkStart = 0; chunkStart < taskJsons.size(); chunkStart += chunkSize) {
                            int chunkEnd = Math.min(chunkStart + chunkSize, taskJsons.size());
                            List<String> chunkJsons = taskJsons.subList(chunkStart, chunkEnd);
                            List<String> chunkNames = taskNames.subList(chunkStart, chunkEnd);

                            for (int i = 0; i < chunkJsons.size(); i++) {
                                String taskName = chunkNames.get(i);
                                String taskJson = chunkJsons.get(i);
                                TaskQueueServiceError.ErrorCode errorCode = TaskProcessor.callCloudTasks(queueName, taskJson, 0L, taskName);
                                responseBuilder.addTaskResult(TaskQueueBulkAddResponse.TaskResult.newBuilder()
                                    .setResult(errorCode)
                                    .setChosenTaskName(ByteString.copyFromUtf8(taskName))
                                    .build());
                            }
                        }
                    }
                    return responseBuilder.build().toByteArray();
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "CLOUDTASK: Error diverting to Cloud Tasks: " + e.getMessage(), e);
                    throw new RuntimeException("CLOUDTASK_DIVERSION_FAILED", e);
                }
            }
        } else if ("taskqueue".equals(packageName) && "Delete".equals(methodName)) {
            if (isCloudTaskBackend()) {
                logger.info("*** CLOUDTASK INTERCEPTED DELETE ***");
                TaskQueueDeleteResponse.Builder responseBuilder = TaskQueueDeleteResponse.newBuilder();
                TaskQueueDeleteRequest deleteRequest = null;
                try {
                    deleteRequest = TaskQueueDeleteRequest.parseFrom(request);
                    
                    String projectId = TaskProcessor.getProjectId();
                    String location = TaskProcessor.getLocation();
                    String queueName = deleteRequest.getQueueName().toStringUtf8();
                    if (queueName == null || queueName.isEmpty()) {
                        queueName = "default";
                    }
                    
                    String fullQueueName = "projects/" + projectId + "/locations/" + location + "/queues/" + queueName;
                    
                    for (int i = 0; i < deleteRequest.getTaskNameCount(); i++) {
                        String taskName = deleteRequest.getTaskName(i).toStringUtf8();
                        TaskProcessor.deleteCloudTask(fullQueueName + "/tasks/" + taskName);
                        responseBuilder.addResult(TaskQueueServiceError.ErrorCode.OK);
                    }
                    return responseBuilder.build().toByteArray();
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "CLOUDTASK: Error diverting delete to Cloud Tasks: " + e.getMessage(), e);
                    int count = (deleteRequest != null) ? deleteRequest.getTaskNameCount() : 1;
                    for (int i = 0; i < count; i++) {
                        responseBuilder.addResult(TaskQueueServiceError.ErrorCode.OK);
                    }
                    return responseBuilder.build().toByteArray();
                }
            }
        } else if ("taskqueue".equals(packageName) && "FetchQueueStats".equals(methodName)) {
            if (isCloudTaskBackend()) {
                logger.info("*** CLOUDTASK INTERCEPTED FETCH STATS ***");
                try {
                    TaskQueueFetchQueueStatsRequest statsRequest = TaskQueueFetchQueueStatsRequest.parseFrom(request);
                    String queueName = (statsRequest.getQueueNameCount() > 0) ? statsRequest.getQueueName(0).toStringUtf8() : "";
                    if (queueName == null || queueName.isEmpty()) {
                        queueName = "default";
                    }
                    
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
                    logger.log(Level.SEVERE, "CLOUDTASK: Failed to fetch stats: " + e.getMessage(), e);
                    throw new RuntimeException("CLOUDTASK_STATS_FAILED", e);
                }
            }
        } else if ("taskqueue".equals(packageName) && "PurgeQueue".equals(methodName)) {
            if (isCloudTaskBackend()) {
                logger.info("*** CLOUDTASK INTERCEPTED PURGE ***");
                try {
                    TaskQueuePurgeQueueRequest purgeRequest = TaskQueuePurgeQueueRequest.parseFrom(request);
                    String queueName = purgeRequest.getQueueName().toStringUtf8();
                    if (queueName == null || queueName.isEmpty()) {
                        queueName = "default";
                    }
                    
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
                        logger.severe("CLOUDTASK: Purge failed with code " + responseCode);
                        throw new RuntimeException("CLOUDTASK: Purge failed with code " + responseCode);
                    }
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "CLOUDTASK: Failed to purge queue: " + e.getMessage(), e);
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
                    logger.info("*** CLOUDTASK: Triggering Fast Path for txn " + txnId + " ***");
                    final List<Long> idsToProcess = new java.util.ArrayList<>(taskIds);
                    ApiProxy.Environment env = ApiProxy.getCurrentEnvironment();
                    CompletableFuture.runAsync(() -> {
                        ApiProxy.setEnvironmentForCurrentThread(env);
                        try {
                            logger.info("*** CLOUDTASK: Fast Path processing for tasks: " + idsToProcess + " ***");
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

    /**
     * Intercepts asynchronous API proxy calls, handling transactional push queue enqueue operations or
     * delegating asynchronous API calls to the underlying API proxy delegate.
     *
     * @param environment the current App Engine API execution environment
     * @param packageName the API service package name (e.g. {@code "taskqueue"})
     * @param methodName the API service method name (e.g. {@code "BulkAdd"})
     * @param request the serialized request protocol buffer bytes
     * @param apiConfig the API execution configuration
     * @return a {@link Future} resolving to the serialized response protocol buffer bytes
     */
    @Override
    public Future<byte[]> makeAsyncCall(ApiProxy.Environment environment, String packageName, String methodName, byte[] request, ApiProxy.ApiConfig apiConfig) {
        logger.fine("*** CLOUDTASK ASYNC CALL: " + packageName + "." + methodName + " ***");
        if ("taskqueue".equals(packageName) && "BulkAdd".equals(methodName)) {
            if (isCloudTaskBackend()) {
                try {
                    TaskQueueBulkAddRequest bulkRequest = TaskQueueBulkAddRequest.parseFrom(request);
                    if (bulkRequest.getAddRequestCount() > 0 && bulkRequest.getAddRequest(0).getMode() == TaskQueueMode.Mode.PULL) {
                        return originalDelegate.makeAsyncCall(environment, packageName, methodName, request, apiConfig);
                    }
                    boolean isTransactional = false;
                    for (TaskQueueAddRequest addReq : bulkRequest.getAddRequestList()) {
                        if (addReq.hasTransaction()) {
                            isTransactional = true;
                            break;
                        }
                    }
                    if (isTransactional) {
                        logger.info("*** CLOUDTASK: Running BulkAdd synchronously for transactional task ***");
                        return java.util.concurrent.CompletableFuture.completedFuture(makeSyncCall(environment, packageName, methodName, request));
                    }
                } catch (Exception e) {
                    logger.warning("*** CLOUDTASK: Failed to parse BulkAdd in makeAsyncCall: " + e.getMessage() + " ***");
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
                logger.info("*** CLOUDTASK: Found txnId " + txnId + " in Commit ***");
            } catch (Exception e) {
                logger.warning("*** CLOUDTASK: Failed to parse Commit request: " + e.getMessage() + " ***");
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
                            logger.info("*** CLOUDTASK: Triggering Fast Path for txn " + finalTxnId + " ***");
                            final List<Long> idsToProcess = new java.util.ArrayList<>(taskIds);
                            ApiProxy.Environment env = ApiProxy.getCurrentEnvironment();
                            CompletableFuture.runAsync(() -> {
                                ApiProxy.setEnvironmentForCurrentThread(env);
                                try {
                                    logger.info("*** CLOUDTASK: Fast Path processing for tasks: " + idsToProcess + " ***");
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

    /**
     * Delegates log record emission to the underlying API proxy delegate.
     *
     * @param environment the current App Engine API execution environment
     * @param record the log record to emit
     */
    @Override
    public void log(ApiProxy.Environment environment, ApiProxy.LogRecord record) {
        originalDelegate.log(environment, record);
    }

    /**
     * Delegates log flushing to the underlying API proxy delegate.
     *
     * @param environment the current App Engine API execution environment
     */
    @Override
    public void flushLogs(ApiProxy.Environment environment) {
        originalDelegate.flushLogs(environment);
    }

    /**
     * Delegates request thread discovery to the underlying API proxy delegate.
     *
     * @param environment the current App Engine API execution environment
     * @return a list of request threads associated with the environment
     */
    @Override
    public List<Thread> getRequestThreads(ApiProxy.Environment environment) {
        return originalDelegate.getRequestThreads(environment);
    }
    private static String buildTaskJson(TaskQueueAddRequest addRequest, String fullQueueName, String taskName, String serviceName) {
        String base64Body = java.util.Base64.getEncoder().encodeToString(addRequest.getBody().toByteArray());
        String relativeUrl = addRequest.getUrl().toStringUtf8();
        long etaUsec = addRequest.getEtaUsec();
        
        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{\"task\": {");
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
                jsonBuilder.append("\"maxAttempts\": ").append(retryParams.getRetryLimit() + 1);
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
        jsonBuilder.append("}}");
        return jsonBuilder.toString();
    }
}
