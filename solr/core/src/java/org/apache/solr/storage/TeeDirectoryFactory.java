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

package org.apache.solr.storage;

import com.codahale.metrics.Meter;
import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.ref.WeakReference;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.MMapDirectoryFactory;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.util.IOFunction;
import org.apache.solr.util.stats.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TeeDirectoryFactory extends MMapDirectoryFactory {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private NodeLevelTeeDirectoryState nodeLevelState;
  private NodeLevelTeeDirectoryState ownNodeLevelState;
  private WeakReference<CoreContainer> cc;

  private String accessDir;
  private boolean useAsyncIO;
  private boolean useDirectIO;

  @Override
  public void initCoreContainer(CoreContainer cc) {
    super.initCoreContainer(cc);
    this.cc = new WeakReference<>(cc);
  }

  static class NodeLevelTeeDirectoryState implements SolrMetricProducer {
    final ExecutorService ioExec = ExecutorUtil.newMDCAwareCachedThreadPool("teeIOExec");
    private final Future<?> activationTask;
    final LinkedBlockingQueue<AccessDirectory.LazyEntry> activationQueue =
        new LinkedBlockingQueue<>();
    final ConcurrentHashMap<AccessDirectory.ConcurrentIntSet, Boolean> priorityActivate =
        new ConcurrentHashMap<>();

    private SolrMetricsContext solrMetricsContext;
    final LongAdder rawCt = new LongAdder();
    final LongAdder loadedCt = new LongAdder();
    final LongAdder populatedCt = new LongAdder();
    final LongAdder lazyCt = new LongAdder();
    final LongAdder lazyMapSize = new LongAdder();
    final LongAdder lazyMapDiskUsage = new LongAdder();
    final LongAdder lazyLoadedBlockBytes = new LongAdder();
    final Meter priorityActivateMeter = new Meter();
    final Meter activateMeter = new Meter();

    NodeLevelTeeDirectoryState() {
      activationTask =
          ioExec.submit(
              () -> {
                Thread t = Thread.currentThread();
                int idleCount = 0; // allow a longer poll interval when nothing's happening
                AccessDirectory.LazyEntry lazyEntry = null;
                while (!t.isInterrupted()) {
                  try {
                    if (!priorityActivate.isEmpty()) {
                      idleCount = 0;
                      Iterator<AccessDirectory.ConcurrentIntSet> iter =
                          priorityActivate.keySet().iterator();
                      while (iter.hasNext()) {
                        priorityActivateMeter.mark(iter.next().call());
                        iter.remove();
                      }
                    } else {
                      if (lazyEntry == null) {
                        lazyEntry = activationQueue.poll(idleCount * 200L, TimeUnit.MILLISECONDS);
                      }
                      if (lazyEntry != null) {
                        // we load background activation in multiple passes, in order to
                        // periodically give
                        // `priorityActivate` a crack at running. Otherwise, a single monolithic
                        // large file
                        // could block IO for a long time, depriving us of the ability to benefit
                        // from
                        // signals about specific file areas that should be loaded earlier.
                        int blocksLoadedCount = lazyEntry.load();
                        if (blocksLoadedCount < 0) {
                          blocksLoadedCount = ~blocksLoadedCount;
                          lazyEntry = null;
                        }
                        activateMeter.mark(blocksLoadedCount);
                        idleCount = 0;
                      } else if (idleCount < 5) {
                        idleCount++;
                      }
                    }
                  } catch (InterruptedException ex) {
                    t.interrupt();
                    return null;
                  } catch (IOException ex) {
                    lazyEntry = null;
                    log.warn("swallowed exception while activating input: {}", ex.toString());
                  } catch (Throwable ex) {
                    lazyEntry = null;
                    log.warn("swallowed unexpected exception while activating input", ex);
                  }
                }
                return null;
              });
    }

    @Override
    public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
      solrMetricsContext = parentContext.getChildContext(this);
      MetricsMap mm =
          new MetricsMap(
              (writer) -> {
                writer.put("rawCt", rawCt.sum());
                writer.put("loadedCt", loadedCt.sum());
                writer.put("populatedCt", populatedCt.sum());
                writer.put("lazyCt", lazyCt.sum());
                writer.put("cumulativeLazyMapSize", lazyMapSize.sum());
                final long diskUsage = lazyMapDiskUsage.sum();
                writer.put("lazyDiskUsage", RamUsageEstimator.humanReadableUnits(diskUsage));
                writer.put("lazyDiskBytesUsed", diskUsage);
                final long blockBytesLoaded = lazyLoadedBlockBytes.sum();
                writer.put(
                    "lazyLoadedBlockUsage", RamUsageEstimator.humanReadableUnits(blockBytesLoaded));
                writer.put("lazyLoadedBlockBytes", blockBytesLoaded);
                BiConsumer<CharSequence, Object> c = writer.getBiConsumer();
                MetricUtils.convertMetric(
                    "priorityActivate",
                    priorityActivateMeter,
                    MetricUtils.ALL_PROPERTIES,
                    false,
                    false,
                    false,
                    false,
                    ":",
                    c::accept);
                MetricUtils.convertMetric(
                    "activate",
                    activateMeter,
                    MetricUtils.ALL_PROPERTIES,
                    false,
                    false,
                    false,
                    false,
                    ":",
                    c::accept);
              });
      solrMetricsContext.gauge(mm, true, scope, SolrInfoBean.Category.DIRECTORY.toString());
    }

    @Override
    public SolrMetricsContext getSolrMetricsContext() {
      return solrMetricsContext;
    }

    @Override
    @SuppressWarnings("try")
    public void close() throws IOException {
      try (Closeable c1 = SolrMetricProducer.super::close;
          Closeable c2 = () -> ExecutorUtil.shutdownAndAwaitTermination(ioExec)) {
        activationTask.cancel(true);
      }
    }
  }

  @Override
  public void init(NamedList<?> args) {
    if (this.cc != null) {
      CoreContainer cc = this.cc.get();
      this.cc = null;
      assert cc != null;
      nodeLevelState =
          cc.getObjectCache()
              .computeIfAbsent(
                  "nodeLevelTeeDirectoryState",
                  NodeLevelTeeDirectoryState.class,
                  (k) -> {
                    NodeLevelTeeDirectoryState ret = new NodeLevelTeeDirectoryState();
                    ret.initializeMetrics(
                        cc.getMetricsHandler().getSolrMetricsContext(), "teeDirectory");
                    return ret;
                  });
    } else {
      nodeLevelState = new NodeLevelTeeDirectoryState();
      ownNodeLevelState = nodeLevelState;
    }
    super.init(args);
    SolrParams params = args.toSolrParams();
    accessDir =
        params.get(
            "accessDir",
            System.getProperty(
                "solr.teeDirectory.accessDir", System.getProperty("java.io.tmpdir")));
    if (!Path.of(accessDir).isAbsolute()) {
      throw new IllegalArgumentException("accessDir should be absolute; found " + accessDir);
    }
    useDirectIO = params.getBool("useDirectIO", CompressingDirectory.DEFAULT_USE_DIRECT_IO);
    useAsyncIO = params.getBool("useAsyncIO", useDirectIO);
  }

  static String getCoreName(String path) {
    assert path.endsWith("/index");
    int end = path.lastIndexOf('/', path.length() - "/index".length() - 1);
    int start = path.lastIndexOf('/', end - 1);
    return path.substring(start, end);
  }

  @Override
  public Directory create(String path, LockFactory lockFactory, DirContext dirContext)
      throws IOException {
    Directory naive = super.create(path, lockFactory, dirContext);
    Path compressedPath = Path.of(path);
    IOFunction<Void, Map.Entry<String, Directory>> accessFunction =
        unused -> {
          String accessPath = accessDir.concat(getCoreName(path));
          Directory dir =
              new AccessDirectory(Path.of(accessPath), lockFactory, compressedPath, nodeLevelState);
          return new AbstractMap.SimpleImmutableEntry<>(accessPath, dir);
        };
    IOFunction<Directory, Map.Entry<Directory, List<String>>> persistentFunction =
        content -> {
          assert content == naive;
          content.close();
          content =
              new CompressingDirectory(
                  compressedPath, nodeLevelState.ioExec, useAsyncIO, useDirectIO);
          return new AbstractMap.SimpleImmutableEntry<>(content, Collections.emptyList());
        };
    return new SizeAwareDirectory(
        new TeeDirectory(naive, accessFunction, persistentFunction, nodeLevelState.ioExec), 0);
  }

  @Override
  @SuppressWarnings("try")
  protected synchronized void removeDirectory(CacheValue cacheValue) throws IOException {
    try (Closeable c = () -> super.removeDirectory(cacheValue)) {
      Directory d = FilterDirectory.unwrap(cacheValue.directory);
      if (d instanceof TeeDirectory) {
        ((TeeDirectory) d).removeAssociated();
      }
    } catch (NoSuchFileException ex) {
      // swallow this. Depending on the order of Directory removal, a parent directory
      // may have removed us first. In any event, the file's not there, which is what
      // we wanted anyway.
    }
  }

  @Override
  @SuppressWarnings("try")
  public void close() throws IOException {
    try (NodeLevelTeeDirectoryState close = ownNodeLevelState) {
      super.close();
    }
  }
}
