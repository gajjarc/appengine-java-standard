package com.google.appengine.api.taskqueue;

import com.google.apphosting.api.ApiProxy;
import java.util.Set;
import javax.servlet.ServletContainerInitializer;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;

public class InterceptorInitializer implements ServletContainerInitializer {
    @Override
    public void onStartup(java.util.Set<Class<?>> c, javax.servlet.ServletContext ctx) throws javax.servlet.ServletException {
        ApiProxy.Delegate originalDelegate = ApiProxy.getDelegate();
        if (originalDelegate != null && !(originalDelegate instanceof InterceptorDelegate)) {
            ApiProxy.setDelegate(new InterceptorDelegate(originalDelegate));
            System.out.println("InterceptorInitializer: Registered InterceptorDelegate successfully.");
        } else {
            System.out.println("InterceptorInitializer: Original delegate is null or already intercepted.");
        }
        
        javax.servlet.ServletRegistration.Dynamic registration = ctx.addServlet("SweeperServlet", SweeperServlet.class);
        if (registration != null) {
            registration.addMapping("/_ah/cloudtask/sweep");
            System.out.println("InterceptorInitializer: Registered SweeperServlet successfully.");
        }
    }
}
