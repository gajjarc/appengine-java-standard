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
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Query;
import com.google.common.base.Ascii;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ReadListener;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet filter that caches the input stream of HTTP POST requests directed to task handlers or
 * the sweeper endpoint.
 *
 * <p>This caching ensures that task payloads can be read reliably across multiple filter or servlet
 * invocations without exhausting the underlying request input stream.
 */
public class RequestCachingFilter implements Filter {
  private static final Logger logger = Logger.getLogger(RequestCachingFilter.class.getName());
  private static final ThreadLocal<List<Long>> PENDING_TASK_IDS = new ThreadLocal<>();

  /**
   * Registers Datastore pending task entity IDs created during the current HTTP request to be
   * automatically dispatched upon request completion.
   *
   * @param taskIds the list of pending task entity IDs
   */
  public static void addPendingTasks(List<Long> taskIds) {
    if (taskIds == null || taskIds.isEmpty()) {
      return;
    }
    List<Long> current = PENDING_TASK_IDS.get();
    if (current != null) {
      current.addAll(taskIds);
    }
  }

  /**
   * Initializes the filter with the specified configuration.
   *
   * @param filterConfig the filter configuration object
   * @throws ServletException if initialization fails
   */
  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    logger.info("RequestCachingFilter: Initialized");
  }

  /**
   * Inspects incoming HTTP POST requests and wraps matching task request streams with a {@link
   * CachedRequestWrapper} to allow repeated stream reads.
   *
   * @param request the incoming servlet request
   * @param response the outgoing servlet response
   * @param chain the filter chain for processing the request
   * @throws IOException if an I/O error occurs during request processing
   * @throws ServletException if a servlet error occurs during request processing
   */
  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    List<Long> taskIds = new ArrayList<>();
    PENDING_TASK_IDS.set(taskIds);
    try {
      if (request instanceof HttpServletRequest httpRequest
          && response instanceof HttpServletResponse httpResponse) {
        String uri = httpRequest.getRequestURI();
        if (uri != null && uri.endsWith(TaskProcessor.SWEEP_ENDPOINT)) {
          handleCloudTaskSweep(httpResponse);
          return;
        }

        // We only need to cache for POST requests which might have payloads
        if (Ascii.equalsIgnoreCase("POST", httpRequest.getMethod())) {
          // Only cache for task handler endpoints to avoid overhead on other requests
          if (uri.contains("/task-handler")) {
            logger.info("RequestCachingFilter: Caching request for URI: " + uri);
            CachedRequestWrapper wrappedRequest = new CachedRequestWrapper(httpRequest);
            chain.doFilter(wrappedRequest, response);
            return;
          }
        }
      }
      chain.doFilter(request, response);
    } finally {
      List<Long> created = PENDING_TASK_IDS.get();
      PENDING_TASK_IDS.remove();
      if (created != null && !created.isEmpty()) {
        try {
          TaskProcessor.processPendingTasks(created, false);
        } catch (Throwable t) {
          logger.log(Level.SEVERE, "Error processing pending tasks in filter: " + t.getMessage(), t);
        }
      }
    }
  }

  private static void handleCloudTaskSweep(HttpServletResponse response) {
    try {
      DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
      Query q =
          new Query("_AE_PendingCloudTask")
              .setFilter(
                  new Query.FilterPredicate(
                      "status",
                      Query.FilterOperator.EQUAL,
                      "PENDING"))
              .setKeysOnly();
      List<Entity> pendingEntities =
          ds.prepare(q).asList(FetchOptions.Builder.withLimit(100));
      List<Long> ids = new ArrayList<>();
      for (Entity e : pendingEntities) {
        ids.add(e.getKey().getId());
      }
      if (!ids.isEmpty()) {
        logger.info("RequestCachingFilter: Sweeper processing " + ids.size() + " pending tasks");
        TaskProcessor.processPendingTasks(ids, true);
      }
      response.setStatus(HttpServletResponse.SC_OK);
      response.getWriter().write("Swept " + ids.size() + " pending tasks");
    } catch (Exception e) {
      logger.log(
          Level.SEVERE, "RequestCachingFilter: Error during /_ah/cloudtask/sweep execution", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }

  /** Cleans up resources held by this filter upon destruction by the servlet container. */
  @Override
  public void destroy() {}

  private static class CachedRequestWrapper extends HttpServletRequestWrapper {
    private final byte[] cachedBody;

    CachedRequestWrapper(HttpServletRequest request) throws IOException {
      super(request);
      try (InputStream is = request.getInputStream();
          ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        is.transferTo(baos);
        this.cachedBody = baos.toByteArray();
      }
    }

    @Override
    public ServletInputStream getInputStream() throws IOException {
      final ByteArrayInputStream bais = new ByteArrayInputStream(cachedBody);
      return new ServletInputStream() {
        @Override
        public boolean isFinished() {
          return bais.available() == 0;
        }

        @Override
        public boolean isReady() {
          return true;
        }

        @Override
        public void setReadListener(ReadListener readListener) {
          throw new UnsupportedOperationException();
        }

        @Override
        public int read() {
          return bais.read();
        }

        @Override
        public int read(byte[] b, int off, int len) {
          return bais.read(b, off, len);
        }
      };
    }

    @Override
    public BufferedReader getReader() throws IOException {
      String encoding = getCharacterEncoding();
      Charset charset =
          (encoding == null || encoding.isEmpty())
              ? StandardCharsets.UTF_8
              : Charset.forName(encoding);
      return new BufferedReader(new InputStreamReader(getInputStream(), charset));
    }
  }
}
