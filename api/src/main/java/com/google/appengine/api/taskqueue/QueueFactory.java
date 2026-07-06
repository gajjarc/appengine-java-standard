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

import com.google.appengine.spi.ServiceFactoryFactory;

/**
 * Creates {@link Queue} objects. {@link QueueFactory} is thread safe.
 *
 */
public final class QueueFactory {
  private static final java.util.logging.Logger log = java.util.logging.Logger.getLogger(QueueFactory.class.getName());

  private static void checkAndRegisterInterceptor() {
    try {
      com.google.apphosting.api.ApiProxy.Delegate originalDelegate = com.google.apphosting.api.ApiProxy.getDelegate();
      if (originalDelegate != null && !(originalDelegate instanceof InterceptorDelegate)) {
        com.google.apphosting.api.ApiProxy.setDelegate(new InterceptorDelegate(originalDelegate));
        log.info("QueueFactory: Registered InterceptorDelegate successfully via getQueue check.");
      } else if (originalDelegate == null) {
        log.warning("QueueFactory: originalDelegate is null, cannot register InterceptorDelegate yet.");
      } else {
        log.fine("QueueFactory: InterceptorDelegate already registered.");
      }
    } catch (Throwable t) {
      log.log(java.util.logging.Level.SEVERE, "QueueFactory: Failed to register InterceptorDelegate", t);
    }
  }

  /** Returns the default {@link Queue}. */
  public static Queue getDefaultQueue() {
    return getQueue(Queue.DEFAULT_QUEUE);
  }

  /**
   * Returns the {@link Queue} by name.
   *
   * <p>The returned {@link Queue} object may not necessarily refer to an existing queue. Queues
   * must be configured before they may be used. Attempting to use a non-existing queue name may
   * result in errors at the point of use of the {@link Queue} object and not when calling {@link
   * #getQueue(String)}.
   */
  public static Queue getQueue(String queueName) {
    checkAndRegisterInterceptor();
    return getFactory().getQueue(queueName);
  }

  private QueueFactory() {}

  private static IQueueFactory getFactory() {
    return ServiceFactoryFactory.getFactory(IQueueFactory.class);
  }
}
