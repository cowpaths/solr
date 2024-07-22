package org.apache.solr.metrics;

import com.codahale.metrics.Clock;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
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

  private static final long INTERVAL = TimeUnit.SECONDS.toNanos(1);

  private static final Function<Clock, Reservoir> DEFAULT_RESERVOIR_FUNCTION =
      (clock) -> new ExponentiallyDecayingReservoir(1028, 0.015, clock);

  private final AtomicLong val = new AtomicLong();

  private final AtomicLong lastUpdate;

  private volatile long lastValue = 0;

  private final AtomicLong maxSinceLastUpdate = new AtomicLong(0);

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

  public static MaxHistogram newInstance() {
    return newInstance(Clock.defaultClock(), DEFAULT_RESERVOIR_FUNCTION);
  }

  public static MaxHistogram newInstance(Clock defaultClock) {
    return newInstance(defaultClock, DEFAULT_RESERVOIR_FUNCTION);
  }

  public static MaxHistogram newInstance(
      Clock clock, Function<Clock, Reservoir> reservoirFunction) {
    RelayClock relayClock = new RelayClock(clock.getTick());
    return new MaxHistogram(reservoirFunction.apply(relayClock), relayClock, clock);
  }

  private MaxHistogram(Reservoir reservoir, RelayClock relayClock, Clock clock) {
    super(reservoir);
    this.relayClock = relayClock;
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
