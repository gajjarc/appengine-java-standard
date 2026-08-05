package com.google.appengine.api.taskqueue;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueFetchQueueStatsResponse;
import com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueScannerQueueInfo;

import com.google.cloud.tasks.v2beta3.AppEngineHttpRequest;
import com.google.cloud.tasks.v2beta3.AppEngineRouting;
import com.google.cloud.tasks.v2beta3.CloudTasksClient;
import com.google.cloud.tasks.v2beta3.CreateTaskRequest;
import com.google.cloud.tasks.v2beta3.DeleteTaskRequest;
import com.google.cloud.tasks.v2beta3.GetQueueRequest;
import com.google.cloud.tasks.v2beta3.Queue;
import com.google.cloud.tasks.v2beta3.QueueName;
import com.google.cloud.tasks.v2beta3.QueueStats;
import com.google.cloud.tasks.v2beta3.Task;
import com.google.cloud.tasks.v2beta3.TaskName;
import com.google.protobuf.ByteString;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Timestamp;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.common.util.concurrent.MoreExecutors;

import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.InvalidArgumentException;
import com.google.api.gax.rpc.FailedPreconditionException;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Clean Java client wrapper for Google Cloud Tasks API operations using official CloudTasksClient SDK.
 * <p>
 * This class provides method-level integration between the legacy App Engine Task Queue API
 * ({@link QueueImpl}) and Google Cloud Tasks when the environment variable
 * {@code APPENGINE_USE_CLOUDTASK_PUSH_QUEUE} is set to {@code true}.
 */
public final class CloudTasksClientWrapper {

    private static final Logger logger = Logger.getLogger(CloudTasksClientWrapper.class.getName());
    private static final String ENV_VAR = "APPENGINE_USE_CLOUDTASK_PUSH_QUEUE";

    static {
        System.setProperty("com.google.cloud.mtls.enabled", "false");
    }

    private static volatile CloudTasksClient sharedClient;

    private CloudTasksClientWrapper() {}

    private static CloudTasksClient getClient() {
        if (sharedClient == null) {
            synchronized (CloudTasksClientWrapper.class) {
                if (sharedClient == null) {
                    try {
                        com.google.appengine.api.appidentity.AppIdentityService appIdentityService =
                            com.google.appengine.api.appidentity.AppIdentityServiceFactory.getAppIdentityService();
                        com.google.appengine.api.appidentity.AppIdentityService.GetAccessTokenResult tokenResult =
                            appIdentityService.getAccessToken(java.util.Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
                        com.google.auth.oauth2.AccessToken accessToken =
                            com.google.auth.oauth2.AccessToken.newBuilder()
                                .setTokenValue(tokenResult.getAccessToken())
                                .setExpirationTime(tokenResult.getExpirationTime())
                                .build();
                        com.google.auth.oauth2.GoogleCredentials credentials =
                            com.google.auth.oauth2.GoogleCredentials.create(accessToken);
                        com.google.cloud.tasks.v2beta3.CloudTasksSettings settings =
                            com.google.cloud.tasks.v2beta3.CloudTasksSettings.newBuilder()
                                .setCredentialsProvider(com.google.api.gax.core.FixedCredentialsProvider.create(credentials))
                                .build();
                        sharedClient = CloudTasksClient.create(settings);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to initialize CloudTasksClient", e);
                    }
                }
            }
        }
        return sharedClient;
    }

    /**
     * Checks whether Cloud Tasks push queue routing is enabled via environment variable.
     *
     * @return {@code true} if Cloud Tasks push queue routing is enabled; {@code false} otherwise.
     */
    public static boolean isEnabled() {
        return Boolean.parseBoolean(System.getenv(ENV_VAR));
    }

    private static String getDefaultServiceName() {
        String serviceName = System.getenv("GAE_SERVICE");
        return (serviceName != null && !serviceName.isEmpty()) ? serviceName : "default";
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

        if (txn != null) {
            return enqueueTransactional(effectiveQueue, txn, taskOptionsList);
        }

        return enqueueNonTransactional(effectiveQueue, taskOptionsList);
    }

    private static Future<List<TaskHandle>> enqueueTransactional(
            String effectiveQueue, Transaction txn, List<TaskOptions> taskOptionsList) {
        String projectId = TaskProcessor.getProjectId();
        String location = TaskProcessor.getLocation();
        QueueName parent = QueueName.of(projectId, location, effectiveQueue);

        List<TaskHandle> createdHandles = new ArrayList<>();
        List<Entity> transactionalEntities = new ArrayList<>();

        for (TaskOptions options : taskOptionsList) {
            String userTaskName = options.getTaskName();
            String pendingName = (userTaskName != null && !userTaskName.isEmpty())
                ? userTaskName : "task-" + UUID.randomUUID();

            String jsonPayload = buildTaskJson(parent.toString(), pendingName, options);

            Entity pendingTask = new Entity("_AE_PendingCloudTask");
            pendingTask.setProperty("queue_name", effectiveQueue);
            pendingTask.setProperty("cloud_task_name", pendingName);
            pendingTask.setProperty("cloud_task_payload", jsonPayload);
            pendingTask.setProperty("created", new java.util.Date());
            pendingTask.setProperty("status", "PENDING");
            pendingTask.setProperty("lock_expires", null);
            pendingTask.setProperty("retry_count", 0L);
            pendingTask.setProperty("last_error", "");
            pendingTask.setProperty("handled_by_sweeper", false);
            pendingTask.setProperty("sdk_lang", "JAVA");
            transactionalEntities.add(pendingTask);

            long scheduleTimeMs = calculateScheduleTimeMs(options);
            TaskOptions handleOptions = new TaskOptions(options);
            handleOptions.taskName(pendingName);
            TaskHandle handle = new TaskHandle(handleOptions, effectiveQueue);
            handle.etaUsec(scheduleTimeMs * 1000L);
            createdHandles.add(handle);
        }

        DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
        List<Key> keys = ds.put(txn, transactionalEntities);
        List<Long> taskIds = new ArrayList<>();
        for (Key k : keys) {
            taskIds.add(k.getId());
        }

        notifyRequestCachingFilterIfPresent(taskIds);

        return CompletableFuture.completedFuture(createdHandles);
    }

    private static void notifyRequestCachingFilterIfPresent(List<Long> taskIds) {
        try {
            Class<?> filterClass = Class.forName("com.google.appengine.api.taskqueue.RequestCachingFilter");
            java.lang.reflect.Method addPendingMethod = filterClass.getMethod("addPendingTasks", List.class);
            addPendingMethod.invoke(null, taskIds);
        } catch (Exception e) {
            logger.log(Level.FINE, "RequestCachingFilter not present or reflective call failed", e);
        }
    }

    private static Future<List<TaskHandle>> enqueueNonTransactional(
            String effectiveQueue, List<TaskOptions> taskOptionsList) {
        String projectId = TaskProcessor.getProjectId();
        String location = TaskProcessor.getLocation();
        String serviceName = getDefaultServiceName();
        QueueName parent = QueueName.of(projectId, location, effectiveQueue);

        try {
            CloudTasksClient client = getClient();
            List<CompletableFuture<TaskHandle>> taskFutures = new ArrayList<>();

            for (TaskOptions options : taskOptionsList) {
                long[] scheduleTimeHolder = new long[1];
                CreateTaskRequest req = buildCreateTaskRequest(
                    parent, projectId, location, effectiveQueue, serviceName, options, scheduleTimeHolder);

                final long finalScheduleTimeMs = scheduleTimeHolder[0];
                final String userTaskName = options.getTaskName();

                CompletableFuture<TaskHandle> cf = new CompletableFuture<>();
                ApiFuture<Task> apiFuture = client.createTaskCallable().futureCall(req);
                ApiFutures.addCallback(apiFuture, new ApiFutureCallback<Task>() {
                    @Override
                    public void onSuccess(Task createdTask) {
                        String chosenTaskName = TaskName.parse(createdTask.getName()).getTask();
                        TaskOptions handleOptions = new TaskOptions(options);
                        handleOptions.taskName(chosenTaskName);
                        TaskHandle handle = new TaskHandle(handleOptions, effectiveQueue);
                        handle.etaUsec(finalScheduleTimeMs * 1000L);
                        cf.complete(handle);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        cf.completeExceptionally(handleCreateTaskError(t, userTaskName, effectiveQueue));
                    }
                }, MoreExecutors.directExecutor());

                taskFutures.add(cf);
            }

            return awaitAllTaskCreationFutures(taskFutures);
        } catch (Exception e) {
            if (e instanceof RuntimeException) throw (RuntimeException) e;
            throw new RuntimeException("Failed to initialize CloudTasksClient", e);
        }
    }

    private static CreateTaskRequest buildCreateTaskRequest(
            QueueName parent, String projectId, String location, String effectiveQueue,
            String serviceName, TaskOptions options, long[] scheduleTimeHolder) {

        AppEngineHttpRequest httpRequest = buildAppEngineHttpRequest(serviceName, options);

        Task.Builder taskBuilder = Task.newBuilder()
            .setAppEngineHttpRequest(httpRequest);

        String userTaskName = options.getTaskName();
        if (userTaskName != null && !userTaskName.isEmpty()) {
            taskBuilder.setName(TaskName.of(projectId, location, effectiveQueue, userTaskName).toString());
        }

        long scheduleTimeMs = calculateScheduleTimeMs(options);
        scheduleTimeHolder[0] = scheduleTimeMs;

        if (isDelayed(options) && scheduleTimeMs > System.currentTimeMillis() + 100L) {
            Timestamp ts = Timestamp.newBuilder()
                .setSeconds(scheduleTimeMs / 1000L)
                .setNanos((int) ((scheduleTimeMs % 1000L) * 1_000_000))
                .build();
            taskBuilder.setScheduleTime(ts);
        }

        return CreateTaskRequest.newBuilder()
            .setParent(parent.toString())
            .setTask(taskBuilder.build())
            .build();
    }

    private static AppEngineHttpRequest buildAppEngineHttpRequest(String serviceName, TaskOptions options) {
        AppEngineHttpRequest.Builder builder = AppEngineHttpRequest.newBuilder()
            .setRelativeUri(options.getUrl() != null && !options.getUrl().isEmpty() ? options.getUrl() : "/")
            .setAppEngineRouting(AppEngineRouting.newBuilder().setService(serviceName).build());

        byte[] payload = options.getPayload();
        if (payload != null && payload.length > 0) {
            builder.setBody(ByteString.copyFrom(payload));
        }

        for (Map.Entry<String, List<String>> entry : options.getHeaders().entrySet()) {
            for (String val : entry.getValue()) {
                builder.putHeaders(entry.getKey(), val);
            }
        }

        applyRetryOptions(builder, options.getRetryOptions());

        return builder.build();
    }

    private static void applyRetryOptions(AppEngineHttpRequest.Builder builder, RetryOptions retryOpts) {
        if (retryOpts == null) return;

        if (retryOpts.getTaskRetryLimit() != null) {
            builder.putHeaders("X-Task-Retry-Limit", String.valueOf(retryOpts.getTaskRetryLimit()));
        }
        if (retryOpts.getTaskAgeLimitSeconds() != null) {
            builder.putHeaders("X-Task-Age-Limit-Seconds", String.valueOf(retryOpts.getTaskAgeLimitSeconds()));
        }
    }

    private static boolean isDelayed(TaskOptions options) {
        return options.getEtaMillis() != null || options.getCountdownMillis() != null;
    }

    private static long calculateScheduleTimeMs(TaskOptions options) {
        long scheduleTimeMs = System.currentTimeMillis();
        if (options.getEtaMillis() != null) {
            scheduleTimeMs = options.getEtaMillis();
        } else if (options.getCountdownMillis() != null) {
            scheduleTimeMs += options.getCountdownMillis();
        }
        return scheduleTimeMs;
    }

    private static Throwable handleCreateTaskError(Throwable t, String userTaskName, String effectiveQueue) {
        String nameForErr = (userTaskName != null && !userTaskName.isEmpty()) ? userTaskName : "unknown";
        if (isAlreadyExists(t)) {
            return new TaskAlreadyExistsException("Task already exists: " + nameForErr);
        } else if (isUnknownQueue(t)) {
            return new IllegalStateException("The specified queue is unknown : " + effectiveQueue, t);
        } else {
            return new RuntimeException("Failed to enqueue task to Cloud Tasks via Client SDK: " + t.getMessage(), t);
        }
    }

    private static Future<List<TaskHandle>> awaitAllTaskCreationFutures(List<CompletableFuture<TaskHandle>> taskFutures) {
        List<TaskHandle> createdHandles = new ArrayList<>();
        TaskAlreadyExistsException taee = null;

        for (CompletableFuture<TaskHandle> cf : taskFutures) {
            try {
                createdHandles.add(cf.join());
            } catch (java.util.concurrent.CompletionException ce) {
                Throwable cause = ce.getCause();
                if (cause instanceof TaskAlreadyExistsException) {
                    if (taee == null) taee = (TaskAlreadyExistsException) cause;
                    else taee.appendTaskName(cause.getMessage());
                } else if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                } else {
                    throw new RuntimeException(cause);
                }
            }
        }
        if (taee != null) throw taee;
        return CompletableFuture.completedFuture(createdHandles);
    }

    /**
     * Asynchronously deletes one or more tasks from Cloud Tasks by task handle concurrently in parallel
     * using official Client SDK.
     *
     * @param queueName the short name of the target App Engine queue
     * @param taskHandles the list of task handles to delete
     * @return a {@link Future} resolving to a list of booleans indicating deletion success
     */
    public static Future<List<Boolean>> deleteTaskAsync(String queueName, List<TaskHandle> taskHandles) {
        String effectiveQueue = (queueName == null || queueName.isEmpty()) ? "default" : queueName;
        String projectId = TaskProcessor.getProjectId();
        String location = TaskProcessor.getLocation();

        try {
            CloudTasksClient client = getClient();
            List<CompletableFuture<Boolean>> futures = new ArrayList<>();
            for (TaskHandle handle : taskHandles) {
                TaskName taskName = TaskName.of(projectId, location, effectiveQueue, handle.getName());
                DeleteTaskRequest req = DeleteTaskRequest.newBuilder().setName(taskName.toString()).build();

                CompletableFuture<Boolean> cf = new CompletableFuture<>();
                ApiFuture<?> apiFuture = client.deleteTaskCallable().futureCall(req);
                ApiFutures.addCallback(apiFuture, new ApiFutureCallback<Object>() {
                    @Override
                    public void onSuccess(Object result) {
                        cf.complete(Boolean.TRUE);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        logger.log(Level.WARNING, "Failed to delete Cloud Task via Client SDK: " + taskName, t);
                        cf.complete(Boolean.FALSE);
                    }
                }, MoreExecutors.directExecutor());
                futures.add(cf);
            }

            List<Boolean> results = new ArrayList<>();
            for (CompletableFuture<Boolean> cf : futures) {
                results.add(cf.join());
            }
            return CompletableFuture.completedFuture(results);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to initialize CloudTasksClient for delete", e);
            List<Boolean> results = new ArrayList<>();
            for (int i = 0; i < taskHandles.size(); i++) results.add(Boolean.FALSE);
            return CompletableFuture.completedFuture(results);
        }
    }

    /**
     * Asynchronously fetches statistics for the specified queue from Cloud Tasks using official Client SDK.
     *
     * @param queueName the short name of the queue to fetch statistics for
     * @return a {@link Future} resolving to the {@link QueueStatistics}
     */
    public static Future<QueueStatistics> fetchStatisticsAsync(String queueName) {
        String effectiveQueue = (queueName == null || queueName.isEmpty()) ? "default" : queueName;
        String projectId = TaskProcessor.getProjectId();
        String location = TaskProcessor.getLocation();

        try {
            CloudTasksClient client = getClient();
            GetQueueRequest request = GetQueueRequest.newBuilder()
                .setName(QueueName.of(projectId, location, effectiveQueue).toString())
                .setReadMask(FieldMask.newBuilder().addPaths("stats").build())
                .build();

            Queue queue = client.getQueue(request);
            QueueStats stats = queue.getStats();

            int tasksCount = (int) stats.getTasksCount();
            long oldestEtaUsec = 0;
            if (stats.hasOldestEstimatedArrivalTime()) {
                Timestamp oldest = stats.getOldestEstimatedArrivalTime();
                oldestEtaUsec = oldest.getSeconds() * 1_000_000L + oldest.getNanos() / 1000L;
            }
            int executedLastMinute = (int) stats.getExecutedLastMinuteCount();
            int requestsInFlight = (int) stats.getConcurrentDispatchesCount();
            double enforcedRate = stats.getEffectiveExecutionRate();

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

            QueueStatistics queueStats = new QueueStatistics(effectiveQueue, legacyStatsBuilder.build());
            return CompletableFuture.completedFuture(queueStats);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "CLOUDTASK: Failed to fetch stats for queue " + effectiveQueue + " via Client SDK: " + e.getMessage(), e);
            throw new RuntimeException("CLOUDTASK_STATS_FAILED", e);
        }
    }

    /**
     * Purges all tasks from the specified Cloud Tasks queue using official Client SDK.
     *
     * @param queueName the short name of the target queue
     */
    public static void purge(String queueName) {
        String effectiveQueue = (queueName == null || queueName.isEmpty()) ? "default" : queueName;
        String projectId = TaskProcessor.getProjectId();
        String location = TaskProcessor.getLocation();
        QueueName parent = QueueName.of(projectId, location, effectiveQueue);

        try {
            CloudTasksClient client = getClient();
            client.purgeQueue(parent);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "CLOUDTASK: Failed to purge queue " + effectiveQueue + " via Client SDK: " + e.getMessage(), e);
            throw new RuntimeException("CLOUDTASK_PURGE_FAILED", e);
        }
    }

    private static boolean isAlreadyExists(Throwable t) {
        Throwable curr = t;
        while (curr != null) {
            if (curr instanceof AlreadyExistsException) return true;
            if (curr instanceof ApiException &&
                ((ApiException) curr).getStatusCode().getCode() == StatusCode.Code.ALREADY_EXISTS) return true;
            String clsName = curr.getClass().getName();
            String msg = curr.getMessage();
            if (clsName.contains("AlreadyExists") || (msg != null && (msg.contains("ALREADY_EXISTS") || msg.contains("existed too recently")))) {
                return true;
            }
            curr = curr.getCause();
        }
        return false;
    }

    private static boolean isUnknownQueue(Throwable t) {
        Throwable curr = t;
        while (curr != null) {
            if (curr instanceof NotFoundException || curr instanceof InvalidArgumentException || curr instanceof FailedPreconditionException) return true;
            if (curr instanceof ApiException) {
                StatusCode.Code code = ((ApiException) curr).getStatusCode().getCode();
                if (code == StatusCode.Code.NOT_FOUND || code == StatusCode.Code.FAILED_PRECONDITION || code == StatusCode.Code.INVALID_ARGUMENT) return true;
            }
            String clsName = curr.getClass().getName();
            String msg = curr.getMessage();
            if (clsName.contains("NotFound") || clsName.contains("InvalidArgument") || clsName.contains("FailedPrecondition") ||
                (msg != null && (msg.contains("Queue does not exist") || msg.contains("NOT_FOUND") || msg.contains("FAILED_PRECONDITION")))) {
                return true;
            }
            curr = curr.getCause();
        }
        return false;
    }

    private static String buildTaskJson(String fullQueueName, String taskName, TaskOptions options) {
        String serviceName = getDefaultServiceName();
        
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
        
        long etaMillis = calculateScheduleTimeMs(options);
        if (etaMillis > System.currentTimeMillis() + 1000L) {
            String isoTime = java.time.Instant.ofEpochMilli(etaMillis).toString();
            jsonBuilder.append(",\"scheduleTime\": \"").append(isoTime).append("\"");
        }
        
        jsonBuilder.append("}}");
        return jsonBuilder.toString();
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
