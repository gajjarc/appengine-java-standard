package com.google.appengine.api.taskqueue;

import com.google.apphosting.api.ApiProxy;
import java.util.Set;
import java.util.logging.Logger;
import javax.servlet.ServletContainerInitializer;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;

/**
 * Servlet container initializer responsible for transparently bootstrapping the Cloud Tasks push queue
 * interceptor and registering background components upon web application startup.
 *
 * <p>Registers the {@link InterceptorDelegate} into App Engine's {@link ApiProxy}, as well as the
 * {@link SweeperServlet} for cron-driven fallback task processing and the {@link RequestCachingFilter}
 * for caching incoming push task request payloads.
 */
public class InterceptorInitializer implements ServletContainerInitializer {
    private static final Logger logger = Logger.getLogger(InterceptorInitializer.class.getName());

    /**
     * Invoked automatically by the Servlet container on web application startup to register
     * API proxy delegates, background worker servlets, and request filters.
     *
     * @param c the set of application classes found that match any criteria specified by annotations
     * @param ctx the servlet context of the web application being initialized
     * @throws ServletException if initialization of servlets or filters fails
     */
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
