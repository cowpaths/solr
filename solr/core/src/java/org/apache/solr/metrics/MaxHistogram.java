package org.apache.solr.metrics;

import com.codahale.metrics.Clock;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A special Histogram that attempts to report once per second, the max value updated over the past
 * second.
 */
public class MaxHistogram extends Histogram {

  private static final long INTERVAL = TimeUnit.SECONDS.toNanos(1);

  private final AtomicLong lastUpdate = new AtomicLong(System.nanoTime());

  private volatile long lastValue = 0;

  private final AtomicLong maxSinceLastUpdate = new AtomicLong(0);

  private final RelayClock clock;

  private static class RelayClock extends Clock {

    private long nextTick = System.nanoTime();

    @Override
    public long getTick() {
      return nextTick;
    }

    public void setTick(long value) {
      nextTick = value;
    }
  }

  public static MaxHistogram newInstance() {
    RelayClock clock = new RelayClock();
    return new MaxHistogram(new ExponentiallyDecayingReservoir(1028, 0.015, clock), clock);
  }

  private MaxHistogram(Reservoir reservoir, RelayClock clock) {
    super(reservoir);
    this.clock = clock;
  }

  @Override
  public Snapshot getSnapshot() {
    maybeFlush(lastValue);
    return super.getSnapshot();
  }

  private boolean maybeFlush(long value) {
    long lastUpdate = this.lastUpdate.get();
    long now = System.nanoTime();
    long diff = now - lastUpdate;
    if (diff >= INTERVAL) {
      int intervalCount = Math.toIntExact(diff / INTERVAL);
      long backdate = lastUpdate + (INTERVAL * intervalCount);
      if (this.lastUpdate.compareAndSet(lastUpdate, backdate)) {
        long maxExtant = maxSinceLastUpdate.getAndSet(value);
        long tick = lastUpdate;
        synchronized (clock) {
          for (int i = intervalCount; i > 0; i--) {
            clock.setTick(tick += INTERVAL);
            super.update(maxExtant);
          }
          clock.setTick(now);
        }
        return true;
      }
    }
    return false;
  }

  @Override
  public void update(final long value) {
    lastValue = value;
    if (maybeFlush(value)) {
      return;
    }
    // either we're not past the interval, or someone else is responsible for
    // calling `super.update()`
    long maxExtant = maxSinceLastUpdate.get();
    long witness = maxExtant;
    while (value > maxExtant
        && (maxExtant = maxSinceLastUpdate.compareAndExchange(witness, value)) != witness) {
      // keep trying
      witness = maxExtant;
    }
  }
}
