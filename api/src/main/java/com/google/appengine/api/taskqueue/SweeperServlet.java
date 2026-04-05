package com.google.appengine.api.taskqueue;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.PreparedQuery;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class SweeperServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        System.out.println("*** CLOUDTASK: Sweeper Cron Triggered ***");
        
        DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
        
        // Query for PENDING tasks
        Query q = new Query("_AE_PendingCloudTask")
            .setFilter(new Query.FilterPredicate("status", Query.FilterOperator.EQUAL, "PENDING"));
            
        PreparedQuery pq = ds.prepare(q);
        List<Long> idsToProcess = new ArrayList<>();
        for (Entity result : pq.asIterable()) {
            idsToProcess.add(result.getKey().getId());
        }
        
        // Also query for PROCESSING tasks that might be stuck (simplified: just process them too)
        Query q2 = new Query("_AE_PendingCloudTask")
            .setFilter(new Query.FilterPredicate("status", Query.FilterOperator.EQUAL, "PROCESSING"));
        PreparedQuery pq2 = ds.prepare(q2);
        for (Entity result : pq2.asIterable()) {
            idsToProcess.add(result.getKey().getId());
        }
        
        if (!idsToProcess.isEmpty()) {
            System.out.println("CLOUDTASK: Sweeper found " + idsToProcess.size() + " tasks to process.");
            TaskProcessor.processPendingTasks(idsToProcess);
        }
        
        resp.getWriter().println("Sweeper completed. Processed " + idsToProcess.size() + " tasks.");
    }
}
