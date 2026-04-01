package com.google.appengine.api.taskqueue;

import com.google.apphosting.api.ApiProxy;
import java.util.List;
import java.util.concurrent.Future;

public class InterceptorDelegate implements ApiProxy.Delegate<ApiProxy.Environment> {
    private final ApiProxy.Delegate<ApiProxy.Environment> originalDelegate;

    @SuppressWarnings("unchecked")
    public InterceptorDelegate(ApiProxy.Delegate<?> originalDelegate) {
        this.originalDelegate = (ApiProxy.Delegate<ApiProxy.Environment>) originalDelegate;
    }

    @Override
    public byte[] makeSyncCall(ApiProxy.Environment environment, String packageName, String methodName, byte[] request) {
        if ("taskqueue".equals(packageName) && "BulkAdd".equals(methodName)) {
            String backend = System.getenv("GAE_PUSHQUEUE_BACKEND");
            if ("CLOUD_TASK".equals(backend)) {
                throw new RuntimeException("CLOUDTASK_INTERCEPTED_ADD_PROXY");
            }
        }
        return originalDelegate.makeSyncCall(environment, packageName, methodName, request);
    }

    @Override
    public Future<byte[]> makeAsyncCall(ApiProxy.Environment environment, String packageName, String methodName, byte[] request, ApiProxy.ApiConfig apiConfig) {
        if ("taskqueue".equals(packageName) && "BulkAdd".equals(methodName)) {
            String backend = System.getenv("GAE_PUSHQUEUE_BACKEND");
            if ("CLOUD_TASK".equals(backend)) {
                throw new RuntimeException("CLOUDTASK_INTERCEPTED_ADD_PROXY");
            }
        }
        return originalDelegate.makeAsyncCall(environment, packageName, methodName, request, apiConfig);
    }

    @Override
    public void log(ApiProxy.Environment environment, ApiProxy.LogRecord record) {
        originalDelegate.log(environment, record);
    }

    @Override
    public void flushLogs(ApiProxy.Environment environment) {
        originalDelegate.flushLogs(environment);
    }

    @Override
    public List<Thread> getRequestThreads(ApiProxy.Environment environment) {
        return originalDelegate.getRequestThreads(environment);
    }
}
