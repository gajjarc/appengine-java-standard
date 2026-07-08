package com.google.appengine.api.taskqueue;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.PreparedQuery;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class SweeperServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String cronHeader = req.getHeader("X-AppEngine-Cron");
        String httpCronHeader = req.getHeader("HTTP_X_APPENGINE_CRON");
        boolean isCron = "true".equalsIgnoreCase(cronHeader) || "true".equalsIgnoreCase(httpCronHeader);
        boolean isDev = System.getProperty("com.google.appengine.runtime.environment", "").equalsIgnoreCase("Development")
                     || System.getProperty("java.class.path", "").contains("appengine-local-runtime");
        if (!isCron && !isDev) {
            resp.sendError(HttpServletResponse.SC_FORBIDDEN, "Access denied: endpoint only accessible via App Engine Cron.");
            return;
        }

        System.out.println("*** CLOUDTASK: Sweeper Cron Triggered ***");
        DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
        Query q = new Query("_AE_PendingCloudTask");
        PreparedQuery pq = ds.prepare(q);

        long now = System.currentTimeMillis();
        List<Long> idsToProcess = new ArrayList<>();
        for (Entity entity : pq.asIterable()) {
            String status = (String) entity.getProperty("status");
            if (status == null || "PENDING".equals(status)) {
                Date created = (Date) entity.getProperty("created");
                if (created != null && (now - created.getTime()) < 60000L) {
                    continue; // Give fast-path 60s to dispatch post-commit
                }
            } else if ("PROCESSING".equals(status)) {
                Date lockExpires = (Date) entity.getProperty("lock_expires");
                if (lockExpires != null && now < lockExpires.getTime()) {
                    continue; // Still actively processing and lock valid
                } else if (lockExpires == null) {
                    continue; // Assume lock valid if just started without timestamp
                }
            } else if ("FAILED".equals(status)) {
                Object retryObj = entity.getProperty("retry_count");
                long retryCount = (retryObj instanceof Number) ? ((Number) retryObj).longValue() : 0L;
                if (retryCount >= 5L) {
                    continue; // Exceeded max sweeper retries
                }
            } else if ("DONE".equals(status) || "ALREADY_EXISTS".equals(status)) {
                continue;
            }
            idsToProcess.add(entity.getKey().getId());
        }

        if (!idsToProcess.isEmpty()) {
            System.out.println("CLOUDTASK: Sweeper found " + idsToProcess.size() + " tasks to process.");
            TaskProcessor.processPendingTasks(idsToProcess, true);
        }

        resp.getWriter().println("Sweeper completed. Processed " + idsToProcess.size() + " tasks.");
    }
}
