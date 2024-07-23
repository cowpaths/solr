package org.apache.solr.metrics;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * A special Histogram that attempts to report once per second, the max value updated over the past
 * second. This is appropriate for lazily tracking ambient values (like "number of outstanding
 * active requests"), where we are interested in:
 *
 * <ul>
 *   <li><i>max</i> values over a unit-granularity time-range (as opposed to arbitrary values that
 *       could be retrieved by sampling)
 *   <li>aggregate values not skewed by activity (i.e., the number of events triggering value
 *       recording should not matter -- thus we want to record values at a constant rate)
 *   <li>not dedicating a thread to recording values at regular intervals
 * </ul>
 */
public class MaxHistogram extends Histogram {

  private static final long INTERVAL_SECONDS = 1;
  private static final long INTERVAL = TimeUnit.SECONDS.toNanos(INTERVAL_SECONDS);

  private final AtomicLong val = new AtomicLong();

  private final AtomicLong lastUpdate;

  private volatile long lastValue = 0;

  private final AtomicLong maxSinceLastUpdate = new AtomicLong(0);

  private final long maxBackdateNanos;

  private final Clock clock;

  private final RelayClock relayClock;

  private static class RelayClock extends Clock {

    private long nextTick;

    private RelayClock(long initialTick) {
      nextTick = initialTick;
    }

    @Override
    public long getTick() {
      return nextTick;
    }

    public void setTick(long value) {
      nextTick = value;
    }
  }

  public static MaxHistogram newInstance(
      int maxBackdateSeconds, Clock clock, Function<Clock, Reservoir> reservoirFunction) {
    RelayClock relayClock = new RelayClock(clock.getTick());
    return new MaxHistogram(
        maxBackdateSeconds, reservoirFunction.apply(relayClock), relayClock, clock);
  }

  public MaxHistogram(int maxBackdateSeconds, Reservoir reservoir, Clock relayClock, Clock clock) {
    super(reservoir);
    if (maxBackdateSeconds == -1) {
      this.maxBackdateNanos = Long.MAX_VALUE;
    } else if (maxBackdateSeconds >= INTERVAL_SECONDS) {
      this.maxBackdateNanos = TimeUnit.SECONDS.toNanos(maxBackdateSeconds);
    } else {
      throw new IllegalArgumentException(
          "maxBackdate must be -1 (unlimited) or >= "
              + INTERVAL_SECONDS
              + "; found "
              + maxBackdateSeconds);
    }
    this.relayClock = (RelayClock) relayClock;
    this.clock = clock;
    this.lastUpdate = new AtomicLong(clock.getTick());
  }

  @Override
  public Snapshot getSnapshot() {
    maybeFlush(lastValue);
    return super.getSnapshot();
  }

  private boolean maybeFlush(long value) {
    long lastUpdate = this.lastUpdate.get();
    long now = clock.getTick();
    long diff = now - lastUpdate;
    if (diff >= INTERVAL) {
      if (diff > maxBackdateNanos) {
        // only update values as far back as `maxBackdateNanos`.
        // This prevents a potential update storm after the value
        // has not been updated in some time.
        diff = maxBackdateNanos;
        lastUpdate = now - maxBackdateNanos;
      }
      int intervalCount = Math.toIntExact(diff / INTERVAL);
      long backdate = lastUpdate + (INTERVAL * intervalCount);
      if (this.lastUpdate.compareAndSet(lastUpdate, backdate)) {
        long maxExtant = maxSinceLastUpdate.getAndSet(value);
        long tick = lastUpdate;
        synchronized (relayClock) {
          for (int i = intervalCount; i > 0; i--) {
            relayClock.setTick(tick += INTERVAL);
            super.update(maxExtant);
          }
          relayClock.setTick(now);
        }
        return true;
      }
    }
    return false;
  }

  @Override
  public void update(final long inc) {
    final long value;
    switch ((int) inc) {
      case 1:
        value = val.incrementAndGet();
        break;
      case -1:
        value = val.decrementAndGet();
        break;
      default:
        throw new IllegalArgumentException();
    }
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
