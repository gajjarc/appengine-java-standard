package com.google.appengine.api.taskqueue;

import com.google.apphosting.api.ApiProxy;
import java.util.Set;
import java.util.logging.Logger;
import javax.servlet.ServletContainerInitializer;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;

public class InterceptorInitializer implements ServletContainerInitializer {
    private static final Logger logger = Logger.getLogger(InterceptorInitializer.class.getName());

    @Override
    public void onStartup(java.util.Set<Class<?>> c, javax.servlet.ServletContext ctx) throws javax.servlet.ServletException {
        ApiProxy.Delegate originalDelegate = ApiProxy.getDelegate();
        if (originalDelegate != null && !(originalDelegate instanceof InterceptorDelegate)) {
            ApiProxy.setDelegate(new InterceptorDelegate(originalDelegate));
            logger.info("InterceptorInitializer: Registered InterceptorDelegate successfully.");
        } else {
            logger.info("InterceptorInitializer: Original delegate is null or already intercepted.");
        }
        
        javax.servlet.ServletRegistration.Dynamic registration = ctx.addServlet("SweeperServlet", SweeperServlet.class);
        if (registration != null) {
            registration.addMapping("/_ah/cloudtask/sweep");
            logger.info("InterceptorInitializer: Registered SweeperServlet successfully.");
        }

        javax.servlet.FilterRegistration.Dynamic filterReg = ctx.addFilter("RequestCachingFilter", RequestCachingFilter.class);
        if (filterReg != null) {
            filterReg.addMappingForUrlPatterns(java.util.EnumSet.of(javax.servlet.DispatcherType.REQUEST), true, "/*");
            logger.info("InterceptorInitializer: Registered RequestCachingFilter successfully.");
        }
    }
}
