package org.apache.solr.servlet;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.servlet.http.HttpServletRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.RateLimiterConfig;

/**
 * PriorityBasedRateLimiter allocates the slot based on their request priority Currently, it has two
 * priorities FOREGROUND and BACKGROUND Requests. Client can pass the {@link
 * org.apache.solr.common.params.CommonParams} SOLR_REQUEST_TYPE_PARAM request header to indicate
 * the foreground and background request. Foreground requests has high priority than background
 * requests
 */
public class PriorityBasedRateLimiter extends RequestRateLimiter {
  public static final String SOLR_REQUEST_PRIORITY_PARAM = "Solr-Request-Priority";
  private final AtomicInteger activeRequests = new AtomicInteger();
  private final Semaphore numRequestsAllowed;

  private final int totalAllowedRequests;

  private final LinkedBlockingQueue<CountDownLatch> waitingList = new LinkedBlockingQueue<>();

  private final long waitTimeoutInMillis;

  public PriorityBasedRateLimiter(RateLimiterConfig rateLimiterConfig) {
    super(rateLimiterConfig);
    this.numRequestsAllowed = new Semaphore(rateLimiterConfig.allowedRequests, true);
    this.totalAllowedRequests = rateLimiterConfig.allowedRequests;
    this.waitTimeoutInMillis = rateLimiterConfig.waitForSlotAcquisition;
  }

  @Override
  public SlotReservation handleRequest(HttpServletRequest request) {
    if (!rateLimiterConfig.isEnabled) {
      return UNLIMITED;
    }
    RequestPriorities requestPriority = getRequestPriority(request);
    if (requestPriority == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Request priority header is not defined or not set properly");
    }
    try {
      if (!acquire(requestPriority)) {
        return null;
      }
    } catch (InterruptedException ie) {
      return null;
    }
    return () -> PriorityBasedRateLimiter.this.release();
  }

  private boolean acquire(RequestPriorities priority) throws InterruptedException {
    if (priority.equals(RequestPriorities.FOREGROUND)) {
      return nextInQueue();
    } else if (priority.equals(RequestPriorities.BACKGROUND)) {
      if (this.activeRequests.get() < this.totalAllowedRequests) {
        return nextInQueue();
      } else {
        CountDownLatch wait = new CountDownLatch(1);
        this.waitingList.put(wait);
        return wait.await(this.waitTimeoutInMillis, TimeUnit.MILLISECONDS) && nextInQueue();
      }
    }
    return true;
  }

  private boolean nextInQueue() throws InterruptedException {
    boolean acquired =
        this.numRequestsAllowed.tryAcquire(1, this.waitTimeoutInMillis, TimeUnit.MILLISECONDS);
    if (!acquired) {
      return false;
    }
    this.activeRequests.addAndGet(1);
    return true;
  }

  private void exitFromQueue() {
    this.numRequestsAllowed.release(1);
    this.activeRequests.addAndGet(-1);
  }

  private void release() {
    if (this.activeRequests.get() > this.totalAllowedRequests) {
      // priority one request is waiting, let's inform it
      this.exitFromQueue();
    } else {
      // next priority
      CountDownLatch waiter = this.waitingList.poll();
      if (waiter != null) {
        waiter.countDown();
      }
      this.exitFromQueue();
    }
  }

  @Override
  public SlotReservation allowSlotBorrowing() throws InterruptedException {
    // if we reach here that means slot is not available
    return null;
  }

  public int getRequestsAllowed() {
    return this.activeRequests.get();
  }

  private RequestPriorities getRequestPriority(HttpServletRequest request) {
    String requestPriority = request.getHeader(SOLR_REQUEST_PRIORITY_PARAM);
    try {
      return RequestPriorities.valueOf(requestPriority);
    } catch (IllegalArgumentException iae) {
    }
    return null;
  }

  public enum RequestPriorities {
    // this has high priority
    FOREGROUND,
    // this has low priority
    BACKGROUND
  }
}
