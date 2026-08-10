/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.appengine.api.taskqueue;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.StatusCode;
import com.google.appengine.api.appidentity.AppIdentityService;
import com.google.appengine.api.appidentity.AppIdentityService.GetAccessTokenResult;
import com.google.appengine.api.appidentity.AppIdentityServiceFactory;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueFetchQueueStatsResponse;
import com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueScannerQueueInfo;
import com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueServiceError.ErrorCode;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.tasks.v2.AppEngineHttpRequest;
import com.google.cloud.tasks.v2.AppEngineRouting;
import com.google.cloud.tasks.v2.CloudTasksClient;
import com.google.cloud.tasks.v2.CloudTasksSettings;
import com.google.cloud.tasks.v2.CreateTaskRequest;
import com.google.cloud.tasks.v2.DeleteTaskRequest;
import com.google.cloud.tasks.v2.GetQueueRequest;
import com.google.cloud.tasks.v2.Queue;
import com.google.cloud.tasks.v2.QueueName;
import com.google.cloud.tasks.v2.Task;
import com.google.cloud.tasks.v2.TaskName;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Clean Java client wrapper for Google Cloud Tasks API operations using official CloudTasksClient
 * SDK.
 *
 * <p>This class provides method-level integration between the legacy App Engine Task Queue API
 * ({@link QueueImpl}) and Google Cloud Tasks when the environment variable {@code
 * APPENGINE_USE_CLOUDTASK_PUSH_QUEUE} is set to {@code true}.
 */
final class CloudTasksClientWrapper {

  private static final Logger logger = Logger.getLogger(CloudTasksClientWrapper.class.getName());
  private static final String ENV_VAR = "APPENGINE_USE_CLOUDTASK_PUSH_QUEUE";

  static {
    System.setProperty("com.google.cloud.mtls.enabled", "false");
  }

  private static volatile CloudTasksClient sharedClient;

  private CloudTasksClientWrapper() {}

  private static volatile String cachedToken = null;
  private static volatile long cachedTokenExpiry = 0;

  private static String getValidAccessToken() throws Exception {
    long now = Instant.now().toEpochMilli();
    if (cachedToken != null && now < cachedTokenExpiry - 60000L) {
      return cachedToken;
    }

    try {
      AppIdentityService appIdentityService =
          AppIdentityServiceFactory.getAppIdentityService();
      GetAccessTokenResult tokenResult =
          appIdentityService.getAccessToken(
              Arrays.asList(
                  "https://www.googleapis.com/auth/cloud-platform",
                  "https://www.googleapis.com/auth/cloudtasks"));
      if (tokenResult != null && tokenResult.getAccessToken() != null) {
        cachedToken = tokenResult.getAccessToken();
        cachedTokenExpiry =
            tokenResult.getExpirationTime() != null
                ? tokenResult.getExpirationTime().toInstant().toEpochMilli()
                : (now + 3600_000L);
        return cachedToken;
      }
    } catch (Throwable ignored) {
      logger.log(Level.FINE, "Could not acquire token from AppIdentityService", ignored);
    }

    try {
      URL url =
          URI.create(
                  "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token?scopes=https://www.googleapis.com/auth/cloud-platform")
              .toURL();
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");
      conn.setRequestProperty("Metadata-Flavor", "Google");
      conn.setConnectTimeout(2000);
      conn.setReadTimeout(2000);
      if (conn.getResponseCode() == 200) {
        try (InputStream is = conn.getInputStream()) {
          String resp = new String(is.readAllBytes(), UTF_8);
          JsonObject obj = JsonParser.parseString(resp).getAsJsonObject();
          String token = obj.get("access_token").getAsString();
          long expiresIn = obj.has("expires_in") ? obj.get("expires_in").getAsLong() : 3600L;
          cachedToken = token;
          cachedTokenExpiry = now + (expiresIn * 1000L);
          return token;
        }
      }
    } catch (Exception e) {
      logger.log(
          Level.WARNING, "Failed to get access token from Metadata server: " + e.getMessage());
    }

    if (cachedToken != null) {
      return cachedToken;
    }
    throw new IOException(
        "Unable to obtain access token from AppIdentityService or Metadata server");
  }

  private static final ReentrantLock CLIENT_INIT_LOCK = new ReentrantLock();

  private static CloudTasksClient getClient() {
    if (sharedClient == null) {
      CLIENT_INIT_LOCK.lock();
      try {
        if (sharedClient == null) {
          CredentialsProvider credentialsProvider =
              () -> {
                try {
                  String token = getValidAccessToken();
                  return GoogleCredentials.create(
                      new AccessToken(token, Date.from(Instant.ofEpochMilli(cachedTokenExpiry))));
                } catch (Exception e) {
                  throw new IOException("Failed to refresh access token", e);
                }
              };
          CloudTasksSettings settings =
              CloudTasksSettings.newBuilder()
                  .setCredentialsProvider(credentialsProvider)
                  .build();
          sharedClient = CloudTasksClient.create(settings);
        }
      } catch (Exception e) {
        throw new IllegalStateException("Failed to initialize CloudTasksClient", e);
      } finally {
        CLIENT_INIT_LOCK.unlock();
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
    return !isNullOrEmpty(serviceName) ? serviceName : "default";
  }

  /**
   * Asynchronously enqueues one or more push tasks to Cloud Tasks or records them in Datastore if
   * transactional.
   *
   * @param queueName the short name of the target App Engine queue
   * @param txn the active Datastore transaction, or {@code null} for non-transactional enqueue
   * @param taskOptionsList the list of task options to enqueue
   * @return a {@link Future} resolving to the list of created {@link TaskHandle}s
   */
  public static Future<List<TaskHandle>> addAsync(
      String queueName, Transaction txn, List<TaskOptions> taskOptionsList) {
    String effectiveQueue = isNullOrEmpty(queueName) ? "default" : queueName;

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
      String pendingName =
          !isNullOrEmpty(userTaskName) ? userTaskName : "task-" + UUID.randomUUID();

      String jsonPayload = buildTaskJson(parent.toString(), pendingName, options);

      Entity pendingTask = new Entity(TaskProcessor.ENTITY_KIND_PENDING_TASK);
      pendingTask.setProperty(TaskProcessor.PROPERTY_QUEUE_NAME, effectiveQueue);
      pendingTask.setProperty(TaskProcessor.PROPERTY_CLOUD_TASK_NAME, pendingName);
      pendingTask.setProperty(TaskProcessor.PROPERTY_CLOUD_TASK_PAYLOAD, jsonPayload);
      pendingTask.setProperty(TaskProcessor.PROPERTY_CREATED, Date.from(Instant.now()));
      pendingTask.setProperty(TaskProcessor.PROPERTY_STATUS, TaskProcessor.STATUS_PENDING);
      pendingTask.setProperty(TaskProcessor.PROPERTY_LOCK_EXPIRES, null);
      pendingTask.setProperty(TaskProcessor.PROPERTY_RETRY_COUNT, 0L);
      pendingTask.setProperty(TaskProcessor.PROPERTY_LAST_ERROR, "");
      pendingTask.setProperty(TaskProcessor.PROPERTY_HANDLED_BY_SWEEPER, false);
      pendingTask.setProperty(TaskProcessor.PROPERTY_SDK_LANG, "JAVA");
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
    TaskProcessor.processPendingTasksAsync(taskIds, 1000L);

    return CompletableFuture.completedFuture(createdHandles);
  }

  private static void notifyRequestCachingFilterIfPresent(List<Long> taskIds) {
    try {
      Class<?> filterClass =
          Class.forName("com.google.appengine.api.taskqueue.RequestCachingFilter");
      java.lang.reflect.Method addPendingMethod =
          filterClass.getMethod("addPendingTasks", List.class);
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
      if (taskOptionsList.size() > 1) {
        JsonObject batchBody = new JsonObject();
        JsonArray requestsArr = new JsonArray();
        List<Long> scheduleTimes = new ArrayList<>();
        List<String> taskNames = new ArrayList<>();

        for (TaskOptions options : taskOptionsList) {
          long scheduleTimeMs = calculateScheduleTimeMs(options);
          scheduleTimes.add(scheduleTimeMs);

          String userTaskName = options.getTaskName();
          String chosenName =
              (userTaskName != null && !userTaskName.isEmpty())
                  ? userTaskName
                  : "task-" + UUID.randomUUID();
          taskNames.add(chosenName);

          JsonObject reqObj = new JsonObject();
          reqObj.addProperty("parent", parent.toString());
          reqObj.add(
              "task",
              buildTaskJsonObject(
                  parent.toString(),
                  serviceName,
                  options,
                  chosenName));
          requestsArr.add(reqObj);
        }
        batchBody.add("requests", requestsArr);

        String restUrl =
            String.format(
                "https://cloudtasks.googleapis.com/v2beta3/projects/%s/locations/%s/queues/%s/tasks:batchCreate",
                projectId, location, effectiveQueue);

        CompletableFuture<List<TaskHandle>> cf = new CompletableFuture<>();
        CompletableFuture<?> unusedBatch =
            CompletableFuture.runAsync(
                () -> {
                  try {
                    String jsonResp = makeRestPost(restUrl, batchBody.toString());
                    JsonObject respObj =
                        JsonParser.parseString(jsonResp).getAsJsonObject();
                    JsonArray createdTasksArr = respObj.getAsJsonArray("tasks");

                    List<TaskHandle> createdHandles = new ArrayList<>();
                    for (int i = 0; i < taskOptionsList.size(); i++) {
                      TaskOptions options = taskOptionsList.get(i);
                      String assignedName = taskNames.get(i);
                      if (createdTasksArr != null && i < createdTasksArr.size()) {
                        JsonObject tObj = createdTasksArr.get(i).getAsJsonObject();
                        if (tObj.has("name")) {
                          String fullName = tObj.get("name").getAsString();
                          assignedName = fullName.substring(fullName.lastIndexOf('/') + 1);
                        }
                      }
                      TaskOptions handleOptions = new TaskOptions(options);
                      handleOptions.taskName(assignedName);
                      TaskHandle handle = new TaskHandle(handleOptions, effectiveQueue);
                      handle.etaUsec(scheduleTimes.get(i) * 1000L);
                      createdHandles.add(handle);
                    }
                    cf.complete(createdHandles);
                  } catch (Throwable t) {
                    cf.completeExceptionally(handleCreateTaskError(t, null, effectiveQueue));
                  }
                });

        return cf;
      }

      List<CompletableFuture<TaskHandle>> taskFutures = new ArrayList<>();

      for (TaskOptions options : taskOptionsList) {
        long scheduleTimeMs = calculateScheduleTimeMs(options);
        String userTaskName = options.getTaskName();

        if (options.getRetryOptions() != null) {
          // Task-level retry options configured: Use REST API
          String chosenName =
              !isNullOrEmpty(userTaskName)
                  ? userTaskName
                  : "task-" + UUID.randomUUID();
          String restUrl =
              String.format(
                  "https://cloudtasks.googleapis.com/v2beta3/projects/%s/locations/%s/queues/%s/tasks",
                  projectId, location, effectiveQueue);
          JsonObject reqObj = new JsonObject();
          reqObj.add(
              "task",
              buildTaskJsonObject(
                  parent.toString(),
                  serviceName,
                  options,
                  chosenName));

          CompletableFuture<TaskHandle> cf = new CompletableFuture<>();
          CompletableFuture<?> unusedSingle =
              CompletableFuture.runAsync(
              () -> {
                try {
                  String jsonResp = makeRestPost(restUrl, reqObj.toString());
                  String assignedName = chosenName;
                  if (!isNullOrEmpty(jsonResp)) {
                    JsonObject respObj = JsonParser.parseString(jsonResp).getAsJsonObject();
                    if (respObj.has("name")) {
                      String fullName = respObj.get("name").getAsString();
                      assignedName = fullName.substring(fullName.lastIndexOf('/') + 1);
                    }
                  }
                  TaskOptions handleOptions = new TaskOptions(options);
                  handleOptions.taskName(assignedName);
                  TaskHandle handle = new TaskHandle(handleOptions, effectiveQueue);
                  handle.etaUsec(scheduleTimeMs * 1000L);
                  cf.complete(handle);
                } catch (Throwable t) {
                  cf.completeExceptionally(handleCreateTaskError(t, userTaskName, effectiveQueue));
                }
              });
          taskFutures.add(cf);
        } else {
          // No task-level retry: Use Client SDK
          long[] scheduleTimeHolder = new long[1];
          CreateTaskRequest req =
              buildCreateTaskRequest(
                  parent,
                  projectId,
                  location,
                  effectiveQueue,
                  serviceName,
                  options,
                  scheduleTimeHolder);

          final long finalScheduleTimeMs = scheduleTimeHolder[0];

          CompletableFuture<TaskHandle> cf = new CompletableFuture<>();
          ApiFuture<Task> apiFuture = client.createTaskCallable().futureCall(req);
          ApiFutures.addCallback(
              apiFuture,
              new ApiFutureCallback<Task>() {
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
              },
              MoreExecutors.directExecutor());

          taskFutures.add(cf);
        }
      }

      return awaitAllTaskCreationFutures(taskFutures);
    } catch (Throwable e) {
      logger.log(
          Level.SEVERE, "CLOUDTASK: Exception in enqueueNonTransactional: " + e.getMessage(), e);
      if (e instanceof RuntimeException re) {
        throw re;
      }
      throw new IllegalStateException("Failed to initialize CloudTasksClient", e);
    }
  }

  private static CreateTaskRequest buildCreateTaskRequest(
      QueueName parent,
      String projectId,
      String location,
      String effectiveQueue,
      String serviceName,
      TaskOptions options,
      long[] scheduleTimeHolder) {

    AppEngineHttpRequest httpRequest = buildAppEngineHttpRequest(serviceName, options);

    Task.Builder taskBuilder = Task.newBuilder().setAppEngineHttpRequest(httpRequest);

    String userTaskName = options.getTaskName();
    if (!isNullOrEmpty(userTaskName)) {
      taskBuilder.setName(
          TaskName.of(projectId, location, effectiveQueue, userTaskName).toString());
    }

    long scheduleTimeMs = calculateScheduleTimeMs(options);
    scheduleTimeHolder[0] = scheduleTimeMs;

    if (isDelayed(options) && scheduleTimeMs > Instant.now().plusMillis(100L).toEpochMilli()) {
      long seconds = scheduleTimeMs / 1000L;
      int nanos = (int) ((scheduleTimeMs % 1000L) * 1_000_000);
      try {
        for (java.lang.reflect.Method m : taskBuilder.getClass().getMethods()) {
          if (m.getName().equals("setScheduleTime") && m.getParameterCount() == 1) {
            Class<?> paramType = m.getParameterTypes()[0];
            if (paramType.getName().endsWith("Timestamp")) {
              Object tsBuilder = paramType.getMethod("newBuilder").invoke(null);
              tsBuilder.getClass().getMethod("setSeconds", long.class).invoke(tsBuilder, seconds);
              tsBuilder.getClass().getMethod("setNanos", int.class).invoke(tsBuilder, nanos);
              Object ts = tsBuilder.getClass().getMethod("build").invoke(tsBuilder);
              m.invoke(taskBuilder, ts);
              break;
            }
          }
        }
      } catch (Exception e) {
        throw new IllegalStateException("Failed to set scheduleTime on Task.Builder", e);
      }
    }

    return CreateTaskRequest.newBuilder()
        .setParent(parent.toString())
        .setTask(taskBuilder.build())
        .build();
  }

  private static AppEngineHttpRequest buildAppEngineHttpRequest(
      String serviceName, TaskOptions options) {
    AppEngineHttpRequest.Builder builder =
        AppEngineHttpRequest.newBuilder()
            .setRelativeUri(!isNullOrEmpty(options.getUrl()) ? options.getUrl() : "/")
            .setAppEngineRouting(AppEngineRouting.newBuilder().setService(serviceName).build());

    byte[] payload = options.getPayload();
    if (payload != null && payload.length > 0) {
      try {
        for (java.lang.reflect.Method m : builder.getClass().getMethods()) {
          if (m.getName().equals("setBody") && m.getParameterCount() == 1) {
            Class<?> paramType = m.getParameterTypes()[0];
            if (paramType.getName().endsWith("ByteString")) {
              java.lang.reflect.Method copyFrom = paramType.getMethod("copyFrom", byte[].class);
              Object bs = copyFrom.invoke(null, payload);
              m.invoke(builder, bs);
              break;
            }
          }
        }
      } catch (Exception e) {
        throw new IllegalStateException("Failed to set body on AppEngineHttpRequest.Builder", e);
      }
    }

    for (Map.Entry<String, List<String>> entry : options.getHeaders().entrySet()) {
      for (String val : entry.getValue()) {
        builder.putHeaders(entry.getKey(), val);
      }
    }

    applyRetryOptions(builder, options.getRetryOptions());

    return builder.build();
  }

  private static void applyRetryOptions(
      AppEngineHttpRequest.Builder builder, RetryOptions retryOpts) {
    if (retryOpts == null) {
      return;
    }

    if (retryOpts.getTaskRetryLimit() != null) {
      builder.putHeaders("X-Task-Retry-Limit", String.valueOf(retryOpts.getTaskRetryLimit()));
    }
    if (retryOpts.getTaskAgeLimitSeconds() != null) {
      builder.putHeaders(
          "X-Task-Age-Limit-Seconds", String.valueOf(retryOpts.getTaskAgeLimitSeconds()));
    }
  }

  private static boolean isDelayed(TaskOptions options) {
    return options.getEtaMillis() != null || options.getCountdownMillis() != null;
  }

  private static long calculateScheduleTimeMs(TaskOptions options) {
    long scheduleTimeMs = Instant.now().toEpochMilli();
    if (options.getEtaMillis() != null) {
      scheduleTimeMs = options.getEtaMillis();
    } else if (options.getCountdownMillis() != null) {
      scheduleTimeMs += options.getCountdownMillis();
    }
    return scheduleTimeMs;
  }

  private static Throwable handleCreateTaskError(
      Throwable t, String userTaskName, String effectiveQueue) {
    String nameForErr = !isNullOrEmpty(userTaskName) ? userTaskName : "unknown";
    if (isAlreadyExists(t)) {
      return new TaskAlreadyExistsException("Task already exists: " + nameForErr);
    } else if (isUnknownQueue(t)) {
      return new IllegalStateException("The specified queue is unknown : " + effectiveQueue, t);
    } else {
      return new RuntimeException(
          "Failed to enqueue task to Cloud Tasks via Client SDK: " + t.getMessage(), t);
    }
  }

  private static Future<List<TaskHandle>> awaitAllTaskCreationFutures(
      List<CompletableFuture<TaskHandle>> taskFutures) {
    List<TaskHandle> createdHandles = new ArrayList<>();
    TaskAlreadyExistsException taee = null;

    for (CompletableFuture<TaskHandle> cf : taskFutures) {
      try {
        createdHandles.add(cf.join());
      } catch (CompletionException ce) {
        Throwable cause = ce.getCause();
        if (cause instanceof TaskAlreadyExistsException taeeCause) {
          if (taee == null) {
            taee = taeeCause;
          } else {
            taee.appendTaskName(taeeCause.getMessage());
          }
        } else if (cause instanceof RuntimeException re) {
          throw re;
        } else {
          throw new IllegalStateException(cause);
        }
      }
    }
    if (taee != null) {
      throw taee;
    }
    return CompletableFuture.completedFuture(createdHandles);
  }

  /**
   * Asynchronously deletes one or more tasks from Cloud Tasks by task handle concurrently in
   * parallel using official Client SDK.
   *
   * @param queueName the short name of the target App Engine queue
   * @param taskHandles the list of task handles to delete
   * @return a {@link Future} resolving to a list of booleans indicating deletion success
   */
  public static Future<List<Boolean>> deleteTaskAsync(
      String queueName, List<TaskHandle> taskHandles) {
    String effectiveQueue = isNullOrEmpty(queueName) ? "default" : queueName;
    String projectId = TaskProcessor.getProjectId();
    String location = TaskProcessor.getLocation();

    try {
      CloudTasksClient client = getClient();
      if (taskHandles.size() > 1) {
        JsonObject batchDeleteBody = new JsonObject();
        JsonArray namesArr = new JsonArray();
        for (TaskHandle handle : taskHandles) {
          namesArr.add(
              TaskName.of(projectId, location, effectiveQueue, handle.getName()).toString());
        }
        batchDeleteBody.add("names", namesArr);

        String restUrl =
            String.format(
                "https://cloudtasks.googleapis.com/v2beta3/projects/%s/locations/%s/queues/%s/tasks:batchDelete",
                projectId, location, effectiveQueue);

        CompletableFuture<List<Boolean>> cf = new CompletableFuture<>();
        CompletableFuture<?> unusedDelete =
            CompletableFuture.runAsync(
                () -> {
                  try {
                    makeRestPost(restUrl, batchDeleteBody.toString());
                    List<Boolean> results = new ArrayList<>();
                    for (int i = 0; i < taskHandles.size(); i++) {
                      results.add(Boolean.TRUE);
                    }
                    cf.complete(results);
                  } catch (Throwable t) {
                    logger.log(Level.WARNING, "Failed to batch delete Cloud Tasks via REST API", t);
                    List<Boolean> results = new ArrayList<>();
                    for (int i = 0; i < taskHandles.size(); i++) {
                      results.add(Boolean.FALSE);
                    }
                    cf.complete(results);
                  }
                });

        return cf;
      }

      List<CompletableFuture<Boolean>> futures = new ArrayList<>();
      for (TaskHandle handle : taskHandles) {
        TaskName taskName = TaskName.of(projectId, location, effectiveQueue, handle.getName());
        DeleteTaskRequest req = DeleteTaskRequest.newBuilder().setName(taskName.toString()).build();

        CompletableFuture<Boolean> cf = new CompletableFuture<>();
        ApiFuture<?> apiFuture = client.deleteTaskCallable().futureCall(req);
        ApiFutures.addCallback(
            apiFuture,
            new ApiFutureCallback<Object>() {
              @Override
              public void onSuccess(Object result) {
                cf.complete(Boolean.TRUE);
              }

              @Override
              public void onFailure(Throwable t) {
                logger.log(
                    Level.WARNING, "Failed to delete Cloud Task via Client SDK: " + taskName, t);
                cf.complete(Boolean.FALSE);
              }
            },
            MoreExecutors.directExecutor());
        futures.add(cf);
      }

      List<Boolean> results = new ArrayList<>();
      for (CompletableFuture<Boolean> cf : futures) {
        results.add(cf.join());
      }
      return CompletableFuture.completedFuture(results);
    } catch (RuntimeException e) {
      logger.log(Level.SEVERE, "Failed to initialize CloudTasksClient for delete", e);
      List<Boolean> results = new ArrayList<>();
      for (int i = 0; i < taskHandles.size(); i++) {
        results.add(Boolean.FALSE);
      }
      return CompletableFuture.completedFuture(results);
    }
  }

  /**
   * Asynchronously fetches statistics for the specified queue from Cloud Tasks using official
   * Client SDK.
   *
   * @param queueName the short name of the queue to fetch statistics for
   * @return a {@link Future} resolving to the {@link QueueStatistics}
   */
  public static Future<QueueStatistics> fetchStatisticsAsync(String queueName) {
    String effectiveQueue = isNullOrEmpty(queueName) ? "default" : queueName;
    String projectId = TaskProcessor.getProjectId();
    String location = TaskProcessor.getLocation();

    CompletableFuture<QueueStatistics> cf = new CompletableFuture<>();
    CompletableFuture<?> unusedStats =
        CompletableFuture.runAsync(
        () -> {
          int tasksCount = 0;
          long oldestEtaUsec = 0;
          int executedLastMinute = 0;
          int requestsInFlight = 0;
          double enforcedRate = 0.0;
          boolean fetched = false;

          // Try Client SDK getQueue first with reflectively constructed FieldMask
          try {
            CloudTasksClient client = getClient();
            GetQueueRequest.Builder reqBuilder =
                GetQueueRequest.newBuilder()
                    .setName(QueueName.of(projectId, location, effectiveQueue).toString());

            for (java.lang.reflect.Method m : reqBuilder.getClass().getMethods()) {
              if (m.getName().equals("setReadMask") && m.getParameterCount() == 1) {
                Class<?> paramType = m.getParameterTypes()[0];
                if (paramType.getName().endsWith("FieldMask")) {
                  Object fmBuilder = paramType.getMethod("newBuilder").invoke(null);
                  fmBuilder
                      .getClass()
                      .getMethod("addPaths", String.class)
                      .invoke(fmBuilder, "stats");
                  Object fm = fmBuilder.getClass().getMethod("build").invoke(fmBuilder);
                  m.invoke(reqBuilder, fm);
                  break;
                }
              }
            }

            Queue queue = client.getQueue(reqBuilder.build());
            if (queue != null) {
              try {
                java.lang.reflect.Method hasStatsMethod = queue.getClass().getMethod("hasStats");
                if (hasStatsMethod.invoke(queue) instanceof Boolean b && b) {
                  Object stats = queue.getClass().getMethod("getStats").invoke(queue);
                  if (stats != null) {
                    tasksCount =
                        ((Number) stats.getClass().getMethod("getTasksCount").invoke(stats))
                            .intValue();
                    try {
                      java.lang.reflect.Method hasOldest =
                          stats.getClass().getMethod("hasOldestEstimatedArrivalTime");
                      if (hasOldest.invoke(stats) instanceof Boolean hasOld && hasOld) {
                        Object oldest =
                            stats
                                .getClass()
                                .getMethod("getOldestEstimatedArrivalTime")
                                .invoke(stats);
                        long seconds =
                            (long) oldest.getClass().getMethod("getSeconds").invoke(oldest);
                        int nanos = (int) oldest.getClass().getMethod("getNanos").invoke(oldest);
                        oldestEtaUsec = seconds * 1_000_000L + nanos / 1000L;
                      }
                    } catch (Exception ex) {
                      logger.log(
                          Level.FINE, "Failed to extract oldest arrival time from QueueStats", ex);
                    }
                    executedLastMinute =
                        ((Number)
                                stats
                                    .getClass()
                                    .getMethod("getExecutedLastMinuteCount")
                                    .invoke(stats))
                            .intValue();
                    requestsInFlight =
                        ((Number)
                                stats
                                    .getClass()
                                    .getMethod("getConcurrentDispatchesCount")
                                    .invoke(stats))
                            .intValue();
                    enforcedRate =
                        ((Number)
                                stats
                                    .getClass()
                                    .getMethod("getEffectiveExecutionRate")
                                    .invoke(stats))
                            .doubleValue();
                    fetched = true;
                  }
                }
              } catch (NoSuchMethodException e) {
                // v2 Queue does not have getStats(), proceed to REST fallback
              }
            }
          } catch (Throwable t) {
            logger.log(
                Level.FINE,
                "Client SDK getQueue failed for stats, falling back to REST: " + t.getMessage(),
                t);
          }

          // If not fetched via Client SDK, fallback to REST GET .../queues/{queue}?readMask=stats
          if (!fetched) {
            try {
              String restUrl =
                  String.format(
                      "https://cloudtasks.googleapis.com/v2beta3/projects/%s/locations/%s/queues/%s?readMask=stats",
                      projectId, location, effectiveQueue);
              String jsonResp = makeRestGet(restUrl);
              JsonObject queueObj =
                  JsonParser.parseString(jsonResp).getAsJsonObject();
              if (queueObj.has("stats")) {
                JsonObject statsObj = queueObj.getAsJsonObject("stats");
                if (statsObj.has("tasksCount")) {
                  tasksCount = statsObj.get("tasksCount").getAsInt();
                }
                if (statsObj.has("oldestEstimatedArrivalTime")) {
                  String isoTime = statsObj.get("oldestEstimatedArrivalTime").getAsString();
                  Instant instant = Instant.parse(isoTime);
                  oldestEtaUsec = instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1000L;
                }
                if (statsObj.has("executedLastMinuteCount")) {
                  executedLastMinute = statsObj.get("executedLastMinuteCount").getAsInt();
                }
                if (statsObj.has("concurrentDispatchesCount")) {
                  requestsInFlight = statsObj.get("concurrentDispatchesCount").getAsInt();
                }
                if (statsObj.has("effectiveExecutionRate")) {
                  enforcedRate = statsObj.get("effectiveExecutionRate").getAsDouble();
                }
              }
              fetched = true;
            } catch (Throwable t) {
              logger.log(
                  Level.WARNING,
                  "Failed to fetch stats for queue " + effectiveQueue + " via REST API",
                  t);
            }
          }

          TaskQueueFetchQueueStatsResponse.QueueStats.Builder legacyStatsBuilder =
              TaskQueueFetchQueueStatsResponse.QueueStats.newBuilder();
          legacyStatsBuilder.setNumTasks(tasksCount);
          legacyStatsBuilder.setOldestEtaUsec(oldestEtaUsec);

          TaskQueueScannerQueueInfo.Builder scannerInfoBuilder =
              TaskQueueScannerQueueInfo.newBuilder();
          scannerInfoBuilder.setExecutedLastMinute(executedLastMinute);
          scannerInfoBuilder.setExecutedLastHour(0);
          scannerInfoBuilder.setRequestsInFlight(requestsInFlight);
          scannerInfoBuilder.setEnforcedRate(enforcedRate);
          scannerInfoBuilder.setSamplingDurationSeconds(60.0);
          legacyStatsBuilder.setScannerInfo(scannerInfoBuilder);

          QueueStatistics queueStats =
              new QueueStatistics(effectiveQueue, legacyStatsBuilder.build());
          cf.complete(queueStats);
        });

    return cf;
  }

  /**
   * Purges all tasks from the specified Cloud Tasks queue using official Client SDK.
   *
   * @param queueName the short name of the target queue
   */
  public static void purge(String queueName) {
    String effectiveQueue = isNullOrEmpty(queueName) ? "default" : queueName;
    String projectId = TaskProcessor.getProjectId();
    String location = TaskProcessor.getLocation();
    QueueName parent = QueueName.of(projectId, location, effectiveQueue);

    try {
      CloudTasksClient client = getClient();
      var unused = client.purgeQueue(parent);
    } catch (RuntimeException e) {
      logger.log(
          Level.SEVERE,
          "CLOUDTASK: Failed to purge queue "
              + effectiveQueue
              + " via Client SDK: "
              + e.getMessage(),
          e);
      throw new IllegalStateException("CLOUDTASK_PURGE_FAILED", e);
    }
  }

  private static boolean isAlreadyExists(Throwable t) {
    Throwable curr = t;
    while (curr != null) {
      if (curr instanceof AlreadyExistsException) {
        return true;
      }
      if (curr instanceof ApiException apiException
          && apiException.getStatusCode().getCode() == StatusCode.Code.ALREADY_EXISTS) {
        return true;
      }
      String clsName = curr.getClass().getName();
      String msg = curr.getMessage();
      if (clsName.contains("AlreadyExists")
          || (msg != null
              && (msg.contains("ALREADY_EXISTS") || msg.contains("existed too recently")))) {
        return true;
      }
      curr = curr.getCause();
    }
    return false;
  }

  private static boolean isUnknownQueue(Throwable t) {
    Throwable curr = t;
    while (curr != null) {
      if (curr instanceof NotFoundException) {
        return true;
      }
      if (curr instanceof ApiException apiException
          && apiException.getStatusCode().getCode() == StatusCode.Code.NOT_FOUND) {
        return true;
      }
      String clsName = curr.getClass().getName();
      String msg = curr.getMessage();
      if (clsName.contains("NotFound")
          || (msg != null
              && (msg.contains("Queue does not exist")
                  || msg.contains("queue not found")
                  || msg.contains("NOT_FOUND")))) {
        return true;
      }
      curr = curr.getCause();
    }
    return false;
  }

  private static String buildTaskJson(String fullQueueName, String taskName, TaskOptions options) {
    String serviceName = getDefaultServiceName();

    byte[] payload = options.getPayload();
    String base64Body =
        (payload != null && payload.length > 0)
            ? Base64.getEncoder().encodeToString(payload)
            : "";

    String relativeUrl = options.getUrl();
    if (isNullOrEmpty(relativeUrl)) {
      relativeUrl = "/";
    }

    StringBuilder jsonBuilder = new StringBuilder();
    jsonBuilder.append("{\"task\": {");
    jsonBuilder
        .append("\"name\": \"")
        .append(fullQueueName)
        .append("/tasks/")
        .append(taskName)
        .append("\",");
    jsonBuilder.append("\"appEngineHttpRequest\": {");
    jsonBuilder.append("\"appEngineRouting\": {");
    jsonBuilder.append("\"service\": \"").append(serviceName).append("\"");
    jsonBuilder.append("},");
    if (options.getMethod() != null) {
      jsonBuilder.append("\"httpMethod\": \"").append(options.getMethod().name()).append("\",");
    }
    jsonBuilder.append("\"relativeUri\": \"").append(escapeJson(relativeUrl)).append("\",");
    if (!base64Body.isEmpty()) {
      jsonBuilder.append("\"body\": \"").append(base64Body).append("\",");
    }
    jsonBuilder.append("\"headers\": {");

    Map<String, String> headers = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : options.getHeaders().entrySet()) {
      for (String val : entry.getValue()) {
        headers.put(entry.getKey(), val);
      }
    }
    if (options.getRetryOptions() != null) {
      RetryOptions ro = options.getRetryOptions();
      if (ro.getTaskRetryLimit() != null) {
        headers.put("X-Task-Retry-Limit", String.valueOf(ro.getTaskRetryLimit()));
      }
      if (ro.getTaskAgeLimitSeconds() != null) {
        headers.put("X-Task-Age-Limit-Seconds", String.valueOf(ro.getTaskAgeLimitSeconds()));
      }
      if (ro.getMinBackoffSeconds() != null) {
        headers.put("X-Task-Min-Backoff-Seconds", String.valueOf(ro.getMinBackoffSeconds()));
      }
      if (ro.getMaxBackoffSeconds() != null) {
        headers.put("X-Task-Max-Backoff-Seconds", String.valueOf(ro.getMaxBackoffSeconds()));
      }
      if (ro.getMaxDoublings() != null) {
        headers.put("X-Task-Max-Doublings", String.valueOf(ro.getMaxDoublings()));
      }
    }

    boolean firstHeader = true;
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      if (!firstHeader) {
        jsonBuilder.append(",");
      }
      jsonBuilder
          .append("\"")
          .append(escapeJson(entry.getKey()))
          .append("\": \"")
          .append(escapeJson(entry.getValue()))
          .append("\"");
      firstHeader = false;
    }

    jsonBuilder.append("}"); // end headers
    jsonBuilder.append("}"); // end appEngineHttpRequest

    long etaMillis = calculateScheduleTimeMs(options);
    if (etaMillis > Instant.now().plusMillis(1000L).toEpochMilli()) {
      String isoTime = Instant.ofEpochMilli(etaMillis).toString();
      jsonBuilder.append(",\"scheduleTime\": \"").append(isoTime).append("\"");
    }

    jsonBuilder.append("}}");
    return jsonBuilder.toString();
  }

  private static String escapeJson(String input) {
    if (input == null) {
      return "";
    }
    return input
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t");
  }

  /**
   * Dispatches a single pending push task stored in Datastore to Google Cloud Tasks via REST API.
   * Encapsulates REST API execution, payload mapping, and error code translation for {@link
   * TaskProcessor}.
   *
   * @param queueName the target task queue name
   * @param payload the JSON task payload stored in Datastore
   * @param entityId the Datastore entity ID for fallback task naming
   * @param taskName the chosen task name or {@code null}
   * @return a {@link
   *     com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueServiceError.ErrorCode}
   */
  public static ErrorCode dispatchPendingTask(
      String queueName, String payload, long entityId, String taskName) {
    String projectId = TaskProcessor.getProjectId();
    String location = TaskProcessor.getLocation();
    String effectiveQueue = isNullOrEmpty(queueName) ? "default" : queueName;
    String restUrl =
        String.format(
            "https://cloudtasks.googleapis.com/v2beta3/projects/%s/locations/%s/queues/%s/tasks",
            projectId, location, effectiveQueue);

    try {
      JsonObject json = JsonParser.parseString(payload).getAsJsonObject();
      JsonObject taskJson = json.has("task") ? json.getAsJsonObject("task") : json;
      JsonObject reqBody = new JsonObject();
      reqBody.add("task", taskJson);

      makeRestPost(restUrl, reqBody.toString());
      logger.info(
          "CLOUDTASK: Successfully dispatched pending task "
              + taskName
              + " to queue "
              + effectiveQueue);
      return ErrorCode.OK;
    } catch (Exception e) {
      if (isAlreadyExists(e)) {
        logger.info("CLOUDTASK: Pending task already exists (idempotency): " + taskName);
        return ErrorCode.TASK_ALREADY_EXISTS;
      } else if (isUnknownQueue(e)) {
        logger.warning("CLOUDTASK: Queue not found: " + queueName);
        return ErrorCode.UNKNOWN_QUEUE;
      } else {
        logger.log(
            Level.SEVERE,
            "CLOUDTASK: Exception dispatching pending task " + taskName + ": " + e.getMessage(),
            e);
        return ErrorCode.INTERNAL_ERROR;
      }
    }
  }

  private static JsonObject buildTaskJsonObject(
      String parent,
      String serviceName,
      TaskOptions options,
      String chosenName) {
    JsonObject taskObj = new JsonObject();
    taskObj.addProperty("name", parent + "/tasks/" + chosenName);

    JsonObject httpReq = new JsonObject();
    httpReq.addProperty(
        "relativeUri",
        !isNullOrEmpty(options.getUrl()) ? options.getUrl() : "/");

    JsonObject routing = new JsonObject();
    routing.addProperty("service", serviceName);
    httpReq.add("appEngineRouting", routing);

    byte[] payload = options.getPayload();
    if (payload != null && payload.length > 0) {
      httpReq.addProperty("body", Base64.getEncoder().encodeToString(payload));
    }

    JsonObject headersObj = new JsonObject();
    for (Map.Entry<String, List<String>> entry : options.getHeaders().entrySet()) {
      if (!entry.getValue().isEmpty()) {
        headersObj.addProperty(entry.getKey(), entry.getValue().get(0));
      }
    }
    if (options.getRetryOptions() != null) {
      RetryOptions ro = options.getRetryOptions();
      if (ro.getTaskRetryLimit() != null) {
        headersObj.addProperty("X-Task-Retry-Limit", String.valueOf(ro.getTaskRetryLimit()));
      }
      if (ro.getTaskAgeLimitSeconds() != null) {
        headersObj.addProperty(
            "X-Task-Age-Limit-Seconds", String.valueOf(ro.getTaskAgeLimitSeconds()));
      }

      JsonObject retryConfig = new JsonObject();
      if (ro.getTaskRetryLimit() != null) {
        retryConfig.addProperty("maxAttempts", ro.getTaskRetryLimit() + 1);
      }
      if (ro.getTaskAgeLimitSeconds() != null) {
        retryConfig.addProperty("maxRetryDuration", ro.getTaskAgeLimitSeconds() + "s");
      }
      if (ro.getMinBackoffSeconds() != null) {
        retryConfig.addProperty("minBackoff", ro.getMinBackoffSeconds() + "s");
      }
      if (ro.getMaxBackoffSeconds() != null) {
        retryConfig.addProperty("maxBackoff", ro.getMaxBackoffSeconds() + "s");
      }
      if (ro.getMaxDoublings() != null) {
        retryConfig.addProperty("maxDoublings", ro.getMaxDoublings());
      }
      if (retryConfig.size() > 0) {
        taskObj.add("retryConfig", retryConfig);
      }
    }
    httpReq.add("headers", headersObj);

    taskObj.add("appEngineHttpRequest", httpReq);

    long scheduleTimeMs = calculateScheduleTimeMs(options);
    if (isDelayed(options) && scheduleTimeMs > Instant.now().plusMillis(100L).toEpochMilli()) {
      Instant instant = Instant.ofEpochMilli(scheduleTimeMs);
      taskObj.addProperty("scheduleTime", instant.toString());
    }

    return taskObj;
  }

  @CanIgnoreReturnValue
  private static String makeRestPost(String urlString, String jsonBody) throws Exception {
    String token = getValidAccessToken();

    URL url = URI.create(urlString).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Authorization", "Bearer " + token);
    conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
    conn.setDoOutput(true);

    if (jsonBody != null && !jsonBody.isEmpty()) {
      try (OutputStream os = conn.getOutputStream()) {
        os.write(jsonBody.getBytes(UTF_8));
      }
    }

    int code = conn.getResponseCode();
    if (code < 200 || code >= 300) {
      String errText = "";
      try (InputStream err = conn.getErrorStream()) {
        if (err != null) {
          errText = new String(err.readAllBytes(), UTF_8);
        }
      }
      throw new IllegalStateException(
          "REST API request to " + urlString + " failed with HTTP " + code + ": " + errText);
    }

    try (InputStream is = conn.getInputStream()) {
      return new String(is.readAllBytes(), UTF_8);
    }
  }

  @CanIgnoreReturnValue
  private static String makeRestGet(String urlString) throws Exception {
    String token = getValidAccessToken();

    URL url = URI.create(urlString).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Authorization", "Bearer " + token);

    int code = conn.getResponseCode();
    if (code < 200 || code >= 300) {
      String errText = "";
      try (InputStream err = conn.getErrorStream()) {
        if (err != null) {
          errText = new String(err.readAllBytes(), UTF_8);
        }
      }
      throw new IllegalStateException(
          "REST API GET to " + urlString + " failed with HTTP " + code + ": " + errText);
    }

    try (InputStream is = conn.getInputStream()) {
      return new String(is.readAllBytes(), UTF_8);
    }
  }

  /**
   * Executes a task force-run / retry via REST API.
   *
   * @param queueName the queue name
   * @param taskName the task name
   * @return a {@link Future} resolving to true if successful
   */
  public static Future<Boolean> runTaskAsync(String queueName, String taskName) {
    String effectiveQueue = isNullOrEmpty(queueName) ? "default" : queueName;
    String projectId = TaskProcessor.getProjectId();
    String location = TaskProcessor.getLocation();

    String restUrl =
        String.format(
            "https://cloudtasks.googleapis.com/v2beta3/projects/%s/locations/%s/queues/%s/tasks/%s:run",
            projectId, location, effectiveQueue, taskName);

    CompletableFuture<Boolean> cf = new CompletableFuture<>();
    CompletableFuture<?> unusedRun =
        CompletableFuture.runAsync(
            () -> {
              try {
                makeRestPost(restUrl, "{}");
                cf.complete(Boolean.TRUE);
              } catch (Throwable t) {
                logger.log(Level.WARNING, "Failed to run/retry task via REST API: " + taskName, t);
                cf.complete(Boolean.FALSE);
              }
            });
    return cf;
  }
}
