package com.google.appengine.api.taskqueue;

import com.google.apphosting.api.ApiProxy;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.CompletableFuture;
import com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueAddRequest;
import com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueBulkAddRequest;
import com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueBulkAddResponse;
import com.google.appengine.api.taskqueue_bytes.TaskQueuePb.TaskQueueServiceError;
import com.google.cloud.tasks.v2.CloudTasksClient;
import com.google.cloud.tasks.v2.CreateTaskRequest;
import com.google.cloud.tasks.v2.QueueName;
import com.google.cloud.tasks.v2.Task;
import com.google.protobuf.ByteString;

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
                System.out.println("*** CLOUDTASK INTERCEPTED ***");
                try {
                    TaskQueueBulkAddRequest bulkRequest = TaskQueueBulkAddRequest.parseFrom(request);
                    TaskQueueBulkAddResponse.Builder responseBuilder = TaskQueueBulkAddResponse.newBuilder();
                    
                    String projectId = "gae-direct-vpc";
                    String location = "us-east1";
                    
                    try (CloudTasksClient client = CloudTasksClient.create()) {
                        for (int i = 0; i < bulkRequest.getAddRequestCount(); i++) {
                            TaskQueueAddRequest addRequest = bulkRequest.getAddRequest(i);
                            String queueName = addRequest.getQueueName().toStringUtf8();
                            String fullQueueName = QueueName.of(projectId, location, queueName).toString();
                            
                            // Modify payload
                            String originalPayload = addRequest.getBody().toStringUtf8();
                            String modifiedPayload = "CLOUDTASK_INTERCEPTED:" + originalPayload;
                            
                            com.google.cloud.tasks.v2.HttpRequest.Builder httpRequestBuilder = 
                                com.google.cloud.tasks.v2.HttpRequest.newBuilder()
                                    .setUrl(addRequest.getUrl().toStringUtf8());
                            
                            // Use reflection to set body to avoid Shading/Repackaging issues
                            try {
                                Class<?> repackagedByteStringClass = Class.forName("com.google.appengine.repackaged.com.google.protobuf.ByteString");
                                java.lang.reflect.Method copyFromUtf8 = repackagedByteStringClass.getMethod("copyFromUtf8", String.class);
                                Object repackagedByteString = copyFromUtf8.invoke(null, modifiedPayload);
                                
                                java.lang.reflect.Method setBody = httpRequestBuilder.getClass().getMethod("setBody", repackagedByteStringClass);
                                setBody.invoke(httpRequestBuilder, repackagedByteString);
                            } catch (Exception re) {
                                // Fallback to standard if not running in repackaged environment
                                System.out.println("CLOUDTASK: Falling back to standard ByteString due to: " + re.getMessage());
                                httpRequestBuilder.setBody(ByteString.copyFromUtf8(modifiedPayload));
                            }
                            
                            Task task = Task.newBuilder()
                                .setHttpRequest(httpRequestBuilder.build())
                                .build();
                                
                            CreateTaskRequest createTaskRequest = CreateTaskRequest.newBuilder()
                                .setParent(fullQueueName)
                                .setTask(task)
                                .build();
                                
                            client.createTask(createTaskRequest);
                                
                            CreateTaskRequest createTaskRequest = CreateTaskRequest.newBuilder()
                                .setParent(fullQueueName)
                                .setTask(task)
                                .build();
                                
                            client.createTask(createTaskRequest);
                            
                            responseBuilder.addTaskResult(TaskQueueBulkAddResponse.TaskResult.newBuilder()
                                .setResult(TaskQueueServiceError.ErrorCode.OK)
                                .build());
                        }
                    }
                    return responseBuilder.build().toByteArray();
                } catch (Exception e) {
                    System.err.println("CLOUDTASK: Error diverting to Cloud Tasks: " + e.getMessage());
                    e.printStackTrace();
                    throw new RuntimeException("CLOUDTASK_DIVERSION_FAILED", e);
                }
            }
        }
        return originalDelegate.makeSyncCall(environment, packageName, methodName, request);
    }

    @Override
    public Future<byte[]> makeAsyncCall(ApiProxy.Environment environment, String packageName, String methodName, byte[] request, ApiProxy.ApiConfig apiConfig) {
        if ("taskqueue".equals(packageName) && "BulkAdd".equals(methodName)) {
            String backend = System.getenv("GAE_PUSHQUEUE_BACKEND");
            if ("CLOUD_TASK".equals(backend)) {
                byte[] responseBytes = makeSyncCall(environment, packageName, methodName, request);
                return CompletableFuture.completedFuture(responseBytes);
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
