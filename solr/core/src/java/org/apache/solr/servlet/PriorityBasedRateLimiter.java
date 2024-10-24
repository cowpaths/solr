package org.apache.solr.servlet;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.core.RateLimiterConfig;

public class PriorityBasedRateLimiter extends RequestRateLimiter {
  private final AtomicInteger priorityOneRequests = new AtomicInteger();
  private final String[] priorities;
  private final Semaphore numRequestsAllowed;

  private final int totalAllowedRequests;

  private final LinkedBlockingQueue<CountDownLatch> waitingList = new LinkedBlockingQueue<>();

  public PriorityBasedRateLimiter(RateLimiterConfig rateLimiterConfig) {
    super(rateLimiterConfig);
    this.priorities =
        new String[] {
          SolrRequest.RequestPriorities.FOREGROUND.toString(),
          SolrRequest.RequestPriorities.BACKGROUND.toString()
        };
    this.numRequestsAllowed = new Semaphore(rateLimiterConfig.priorityMaxRequests, true);
    this.totalAllowedRequests = rateLimiterConfig.priorityMaxRequests;
  }

  /* public PriorityBasedRequestLimiter(String[] priorities, int numRequestsAllowed) {
      this.priorities = priorities;
      this.numRequestsAllowed = new Semaphore(numRequestsAllowed, true);
      this.totalAllowedRequests = numRequestsAllowed;
  }*/

  @Override
  public SlotReservation handleRequest(String requestPriority) throws InterruptedException {
    acquire(requestPriority);
    return () -> PriorityBasedRateLimiter.this.release(requestPriority);
  }

  public void acquire(String priority) throws InterruptedException {
    if (priority.equals(this.priorities[0])) {
      nextInQueue();
    } else if (priority.equals(this.priorities[1])) {
      if (this.priorityOneRequests.get() < this.totalAllowedRequests) {
        nextInQueue();
      } else {
        CountDownLatch wait = new CountDownLatch(1);
        this.waitingList.put(wait);
        wait.await();
        nextInQueue();
      }
    }
  }

  private void nextInQueue() throws InterruptedException {
    this.priorityOneRequests.addAndGet(1);
    this.numRequestsAllowed.acquire(1);
  }

  private void exitFromQueue() {
    this.priorityOneRequests.addAndGet(-1);
    this.numRequestsAllowed.release(1);
  }

  public void release(String priority) {
    if (this.priorities[0].equals(priority) || this.priorities[1].equals(priority)) {
      if (this.priorityOneRequests.get() > this.totalAllowedRequests) {
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
  }

  @Override
  public SlotReservation allowSlotBorrowing() throws InterruptedException {
    throw new RuntimeException(
        "PriorityBasedRateLimiter.allowSlotBorrowing method is not implemented");
  }

  public int getRequestsAllowed() {
    return this.priorityOneRequests.get();
  }
}
