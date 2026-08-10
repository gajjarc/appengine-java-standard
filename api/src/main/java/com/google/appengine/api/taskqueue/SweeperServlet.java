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

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.common.base.Ascii;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;
import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet endpoint invoked by App Engine Cron to periodically query and process pending Cloud Tasks
 * stored in Datastore (`_AE_PendingCloudTask`).
 *
 * <p>Acts as a reliable background sweeper to recover and dispatch transactional or delayed push
 * tasks that were not immediately dispatched by fast-path execution.
 */
public class SweeperServlet implements Servlet {
  private static final Logger logger = Logger.getLogger(SweeperServlet.class.getName());
  private ServletConfig config;

  @Override
  public void init(ServletConfig config) throws ServletException {
    this.config = config;
  }

  @Override
  public ServletConfig getServletConfig() {
    return config;
  }

  @Override
  public void service(ServletRequest request, ServletResponse response)
      throws ServletException, IOException {
    if (request instanceof HttpServletRequest req && response instanceof HttpServletResponse resp) {
      doGet(req, resp);
    }
  }

  @Override
  public String getServletInfo() {
    return "App Engine Cloud Tasks Sweeper Servlet";
  }

  @Override
  public void destroy() {}

  /**
   * Handles HTTP GET requests from App Engine Cron to sweep pending task entities from Datastore
   * and dispatch them to Cloud Tasks.
   *
   * @param req the HTTP request issued by the App Engine Cron infrastructure
   * @param resp the HTTP response returned to the cron infrastructure
   * @throws ServletException if a servlet processing exception occurs
   * @throws IOException if an I/O error occurs writing the response
   */
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String cronHeader = req.getHeader(TaskProcessor.HEADER_CRON);
    String httpCronHeader = req.getHeader(TaskProcessor.HEADER_HTTP_CRON);
    boolean isCron =
        Ascii.equalsIgnoreCase("true", cronHeader)
            || Ascii.equalsIgnoreCase("true", httpCronHeader);
    boolean isDev =
        Ascii.equalsIgnoreCase(
                System.getProperty("com.google.appengine.runtime.environment", ""), "Development")
            || System.getProperty("java.class.path", "").contains("appengine-local-runtime");
    if (!isCron && !isDev) {
      resp.sendError(
          HttpServletResponse.SC_FORBIDDEN,
          "Access denied: endpoint only accessible via App Engine Cron.");
      return;
    }

    logger.info("*** CLOUDTASK: Sweeper Cron Triggered ***");
    DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
    Query q = new Query(TaskProcessor.ENTITY_KIND_PENDING_TASK);
    PreparedQuery pq = ds.prepare(q);

    long now = Instant.now().toEpochMilli();
    List<Long> idsToProcess = new ArrayList<>();
    for (Entity entity : pq.asIterable()) {
      String status = (String) entity.getProperty(TaskProcessor.PROPERTY_STATUS);
      if (status == null || Objects.equals(status, TaskProcessor.STATUS_PENDING)) {
        Object createdObj = entity.getProperty(TaskProcessor.PROPERTY_CREATED);
        Instant created = (createdObj instanceof Date d) ? d.toInstant() : null;
        if (created != null && (now - created.toEpochMilli()) < 60000L) {
          continue; // Give fast-path 60s to dispatch post-commit
        }
      } else if (Objects.equals(status, TaskProcessor.STATUS_PROCESSING)) {
        Object lockObj = entity.getProperty(TaskProcessor.PROPERTY_LOCK_EXPIRES);
        Instant lockExpires = (lockObj instanceof Date d) ? d.toInstant() : null;
        if (lockExpires != null && now < lockExpires.toEpochMilli()) {
          continue; // Still actively processing and lock valid
        } else if (lockExpires == null) {
          continue; // Assume lock valid if just started without timestamp
        }
      } else if (Objects.equals(status, TaskProcessor.STATUS_FAILED)) {
        Object retryObj = entity.getProperty(TaskProcessor.PROPERTY_RETRY_COUNT);
        long retryCount = (retryObj instanceof Number num) ? num.longValue() : 0L;
        if (retryCount >= 5L) {
          continue; // Exceeded max sweeper retries
        }
      } else if (Objects.equals(status, TaskProcessor.STATUS_DONE)
          || Objects.equals(status, TaskProcessor.STATUS_ALREADY_EXISTS)) {
        continue;
      }
      idsToProcess.add(entity.getKey().getId());
    }

    if (!idsToProcess.isEmpty()) {
      logger.info("CLOUDTASK: Sweeper found " + idsToProcess.size() + " tasks to process.");
      TaskProcessor.processPendingTasks(idsToProcess, true);
    }

    resp.getWriter().println("Sweeper completed. Processed " + idsToProcess.size() + " tasks.");
  }
}
