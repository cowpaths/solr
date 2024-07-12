/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.metrics;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.solr.core.PluginInfo;

/**
 * This class represents a metrics manager that is delegate aware in that it is aware of multiple
 * metric registries, a primary and a delegate. This enables creating metrics that are tracked at
 * multiple levels, i.e. core-level and node-level. This class will create instances of new Timer,
 * Meter, Counter, Histogram implementations that hold references to both primary and delegate
 * implementations of corresponding classes. The DelegateRegistry* metric classes are just
 * pass-through to two different implementations. As such the DelegateRegistry* metric classes do
 * not hold any metric data themselves.
 *
 * @see org.apache.solr.metrics.SolrMetricsContext
 */
public class SolrDelegateMetricManager extends SolrMetricManager {

  public static SolrMetricManager delegatingMetricManager(
      SolrMetricManager mgr, String delegateRegistry) {
    if (mgr instanceof SolrDelegateMetricManager) {
      assert delegateRegistry.equals(((SolrDelegateMetricManager) mgr).delegateRegistry);
      return mgr;
    }
    return new SolrDelegateMetricManager(delegateRegistry, mgr);
  }

  private final String delegateRegistry;

  private SolrDelegateMetricManager(String delegateRegistry, SolrMetricManager delegate) {
    super(delegate);
    this.delegateRegistry = delegateRegistry;
  }

  @Override
  public MetricRegistry registry(String registry) {
    return new DelegateMetricRegistry(
        super.registry(registry),
        super.registry(delegateRegistry),
        getMetricsConfig().getTimerSupplier());
  }

  private static class DelegateMetricRegistry extends MetricRegistry {
    private final MetricRegistry primary;
    private final MetricRegistry delegate;
    private final PluginInfo timerSupplier;

    private DelegateMetricRegistry(
        MetricRegistry primary, MetricRegistry delegate, PluginInfo timerSupplier) {
      this.primary = primary;
      this.delegate = delegate;
      this.timerSupplier = timerSupplier;
    }

    @Override
    public Histogram histogram(String name, MetricSupplier<Histogram> supplier) {
      return new DelegateRegistryHistogram(
          primary.histogram(name, supplier), delegate.histogram(name, supplier));
    }

    @Override
    public Meter meter(String name, MetricSupplier<Meter> supplier) {
      return new DelegateRegistryMeter(
          primary.meter(name, supplier), delegate.meter(name, supplier));
    }

    @Override
    public Timer timer(String name, MetricSupplier<Timer> supplier) {
      Clock clock = MetricSuppliers.getClock(timerSupplier, MetricSuppliers.CLOCK);
      return new DelegateRegistryTimer(
          clock, primary.timer(name, supplier), delegate.timer(name, supplier));
    }

    @Override
    public Counter counter(String name, MetricSupplier<Counter> supplier) {
      return new DelegateRegistryCounter(
          primary.counter(name, supplier), delegate.counter(name, supplier));
    }
  }
}
