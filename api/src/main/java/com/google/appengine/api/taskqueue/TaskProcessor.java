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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueServiceError.ErrorCode;
import com.google.apphosting.api.ApiProxy;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Processor utility responsible for executing and dispatching pending Cloud Tasks stored in
 * Datastore ({@code _AE_PendingCloudTask}) to Google Cloud Tasks using the official Client SDK
 * wrapper.
 *
 * <p>Handles task state transitions ({@code PENDING}, {@code PROCESSING}, {@code DONE}, {@code
 * FAILED}), exponential backoff retry tracking, GCP project and region discovery, and task
 * dispatching via {@link CloudTasksClientWrapper}.
 */
final class TaskProcessor {
  private static final Logger logger = Logger.getLogger(TaskProcessor.class.getName());

  private TaskProcessor() {}

  // Datastore Entity Kind & Properties
  static final String ENTITY_KIND_PENDING_TASK = "_AE_PendingCloudTask";
  static final String PROPERTY_QUEUE_NAME = "queue_name";
  static final String PROPERTY_CLOUD_TASK_NAME = "cloud_task_name";
  static final String PROPERTY_CLOUD_TASK_PAYLOAD = "cloud_task_payload";
  static final String PROPERTY_CREATED = "created";
  static final String PROPERTY_STATUS = "status";
  static final String PROPERTY_LOCK_EXPIRES = "lock_expires";
  static final String PROPERTY_RETRY_COUNT = "retry_count";
  static final String PROPERTY_LAST_ERROR = "last_error";
  static final String PROPERTY_LAST_RES_CODE = "last_res_code";
  static final String PROPERTY_NEXT_RETRY_AT = "next_retry_at";
  static final String PROPERTY_HANDLED_BY_SWEEPER = "handled_by_sweeper";
  static final String PROPERTY_SDK_LANG = "sdk_lang";

  // Task Statuses
  static final String STATUS_PENDING = "PENDING";
  static final String STATUS_PROCESSING = "PROCESSING";
  static final String STATUS_DONE = "DONE";
  static final String STATUS_FAILED = "FAILED";
  static final String STATUS_ALREADY_EXISTS = "ALREADY_EXISTS";

  // Sweeper & Cron Endpoints / Headers
  static final String SWEEP_ENDPOINT = "/_ah/cloudtask/sweep";
  static final String HEADER_CRON = "X-AppEngine-Cron";
  static final String HEADER_HTTP_CRON = "HTTP_X_APPENGINE_CRON";

  // Environment Variables & System Properties
  static final String ENV_GAE_ZONE = "GAE_ZONE";
  static final String ENV_LOCATION_ID = "LOCATION_ID";
  static final String ENV_GAE_LOCATION = "GAE_LOCATION";
  static final String ENV_GAE_REGION = "GAE_REGION";
  static final String PROP_GAE_LOCATION = "gae.location";

  /**
   * Asynchronously processes a list of pending task entity IDs after an optional delay.
   *
   * @param ids the list of Datastore entity IDs for {@code _AE_PendingCloudTask} entities
   * @param delayMillis milliseconds to wait before executing (e.g., to allow caller transaction to
   *     commit)
   */
  public static void processPendingTasksAsync(List<Long> ids, long delayMillis) {
    if (ids == null || ids.isEmpty()) {
      return;
    }
    ApiProxy.Environment env = ApiProxy.getCurrentEnvironment();
    CompletableFuture<?> unused =
        CompletableFuture.runAsync(
            () -> {
              if (env != null) {
                ApiProxy.setEnvironmentForCurrentThread(env);
              }
              try {
                if (delayMillis > 0) {
                  try {
                    Thread.sleep(delayMillis);
                  } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                  }
                }
                processPendingTasks(ids, false);
              } catch (Throwable t) {
                logger.log(Level.SEVERE, "Error in processPendingTasksAsync: " + t.getMessage(), t);
              } finally {
                if (env != null) {
                  ApiProxy.clearEnvironmentForCurrentThread();
                }
              }
            });
  }

  /**
   * Processes a list of pending task entity IDs stored in Datastore.
   *
   * @param ids the list of Datastore entity IDs for {@code _AE_PendingCloudTask} entities to
   *     process
   */
  public static void processPendingTasks(List<Long> ids) {
    processPendingTasks(ids, false);
  }

  /**
   * Processes a list of pending task entity IDs stored in Datastore, indicating whether invocation
   * originated from the background sweeper cron job.
   *
   * @param ids the list of Datastore entity IDs for {@code _AE_PendingCloudTask} entities to
   *     process
   * @param handledBySweeper {@code true} if triggered by the cron sweeper; {@code false} if
   *     triggered by fast-path
   */
  public static void processPendingTasks(List<Long> ids, boolean handledBySweeper) {
    DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
    for (Long id : ids) {
      Key key = KeyFactory.createKey(ENTITY_KIND_PENDING_TASK, id);
      try {
        processSingleTask(ds, key, handledBySweeper);
      } catch (RuntimeException e) {
        logger.log(Level.SEVERE, "Failed to process pending task " + id + ": " + e.getMessage(), e);
      }
    }
  }

  private static void processSingleTask(DatastoreService ds, Key key, boolean handledBySweeper) {
    Entity entity = null;
    try {
      entity = ds.get(key);
    } catch (EntityNotFoundException enfe) {
      // Entity was deleted or rolled back
      return;
    }

    String status = (String) entity.getProperty(PROPERTY_STATUS);
    if (Objects.equals(status, STATUS_DONE) || Objects.equals(status, STATUS_ALREADY_EXISTS)) {
      return;
    }

    Object lockObj = entity.getProperty(PROPERTY_LOCK_EXPIRES);
    Instant lockExpires = (lockObj instanceof Date d) ? d.toInstant() : null;
    if (Objects.equals(status, STATUS_PROCESSING) && lockExpires != null) {
      if (Instant.now().isBefore(lockExpires)) {
        // Still actively processing
        return;
      }
    }

    entity.setProperty(PROPERTY_STATUS, STATUS_PROCESSING);
    entity.setProperty(
        PROPERTY_LOCK_EXPIRES, Date.from(Instant.now().plus(Duration.ofMinutes(1))));
    entity.setProperty(PROPERTY_HANDLED_BY_SWEEPER, handledBySweeper);
    try {
      ds.put(entity);
    } catch (RuntimeException e) {
      logger.log(Level.WARNING, "Failed to acquire lock for task " + key.getId() + ": " + e.getMessage());
      return;
    }

    String queueName = (String) entity.getProperty(PROPERTY_QUEUE_NAME);
    String payload = (String) entity.getProperty(PROPERTY_CLOUD_TASK_PAYLOAD);
    long entityId = key.getId();

    boolean success = false;
    ErrorCode resCode = ErrorCode.INTERNAL_ERROR;
    try {
      resCode =
          callCloudTasksViaSdk(
              queueName, payload, entityId, (String) entity.getProperty(PROPERTY_CLOUD_TASK_NAME));
      success = (resCode == ErrorCode.OK || resCode == ErrorCode.TASK_ALREADY_EXISTS);
    } catch (RuntimeException ex) {
      logger.log(
          Level.SEVERE,
          "CLOUDTASK: Exception during Client SDK dispatch for task "
              + entityId
              + ": "
              + ex.getMessage(),
          ex);
      success = false;
    }

    if (success) {
      try {
        ds.delete(key);
        logger.info("CLOUDTASK: Successfully processed and cleaned up task " + entityId);
      } catch (RuntimeException e) {
        logger.warning("Failed to clean up task entity " + entityId + ": " + e.getMessage());
      }
    } else {
      try {
        Object retryObj = entity.getProperty(PROPERTY_RETRY_COUNT);
        long retryCount = (retryObj instanceof Number num) ? num.longValue() : 0L;
        retryCount++;
        entity.setProperty(PROPERTY_RETRY_COUNT, retryCount);
        entity.setProperty(
            PROPERTY_LAST_ERROR, "Cloud Tasks Client SDK call failed with " + resCode);
        if (retryCount >= 5L) {
          entity.setProperty(PROPERTY_STATUS, STATUS_FAILED);
        } else {
          entity.setProperty(PROPERTY_STATUS, STATUS_PENDING);
        }
        entity.setProperty(PROPERTY_LOCK_EXPIRES, null);
        ds.put(entity);
        logger.warning(
            "CLOUDTASK: Failed to process task " + entityId + ", retry count: " + retryCount);
      } catch (RuntimeException putErr) {
        logger.severe("Failed to record error state for task " + entityId + ": " + putErr.getMessage());
      }
    }
  }

  /**
   * Resolves the current Google Cloud Platform project ID from the App Engine runtime environment.
   *
   * @return the GCP project ID string
   */
  public static String getProjectId() {
    ApiProxy.Environment env = ApiProxy.getCurrentEnvironment();
    if (env != null) {
      String appId = env.getAppId();
      if (appId != null && appId.contains("~")) {
        return appId.substring(appId.indexOf("~") + 1);
      }
      return (appId != null) ? appId : "";
    }
    return "";
  }

  /**
   * Resolves the current App Engine deployment location/region from environment variables, system
   * properties, or the GCP instance metadata server.
   *
   * @return the GCP region ID (e.g. {@code "us-central1"}, {@code "us-east1"})
   */
  public static String getLocation() {
    String zone = System.getenv(ENV_GAE_ZONE);
    if (zone != null && !zone.isEmpty()) {
      int lastDash = zone.lastIndexOf('-');
      if (lastDash > 0) {
        return zone.substring(0, lastDash);
      }
      return zone;
    }
    String location = System.getenv(ENV_LOCATION_ID);
    if (location != null && !location.isEmpty()) {
      return location;
    }
    location = System.getenv(ENV_GAE_LOCATION);
    if (location != null && !location.isEmpty()) {
      return location;
    }
    location = System.getenv(ENV_GAE_REGION);
    if (location != null && !location.isEmpty()) {
      return location;
    }
    location = System.getProperty(PROP_GAE_LOCATION);
    if (location != null && !location.isEmpty()) {
      return location;
    }
    try {
      URL url = URI.create("http://metadata.google.internal/computeMetadata/v1/instance/region").toURL();
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestProperty("Metadata-Flavor", "Google");
      conn.setConnectTimeout(2000);
      conn.setReadTimeout(2000);
      if (conn.getResponseCode() == 200) {
        try (BufferedReader reader =
            new BufferedReader(new InputStreamReader(conn.getInputStream(), UTF_8))) {
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
   * @return a {@link ErrorCode} indicating success or failure code
   */
  public static ErrorCode callCloudTasksViaSdk(
      String queueName, String payload, long entityId, String taskName) {
    return CloudTasksClientWrapper.dispatchPendingTask(queueName, payload, entityId, taskName);
  }
}
