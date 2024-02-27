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

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.commons.io.file.PathUtils;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.util.IOFunction;

public class TeeDirectory extends BaseDirectory {

  private volatile Directory access;
  private final ExecutorService ioExec;
  private final AutoCloseable closeLocal;
  private final IOFunction<Void, Map.Entry<String, Directory>> accessFunction;
  private final IOFunction<Directory, Map.Entry<Directory, List<String>>> persistentFunction;
  private Directory persistent;

  /**
   * This ctor (with default inline config) exists to be invoked during testing, via
   * MockDirectoryFactory.
   */
  public TeeDirectory(Path path, LockFactory lockFactory) throws IOException {
    super(TEE_LOCK_FACTORY);
    TeeDirectoryFactory.NodeLevelTeeDirectoryState ownState =
        new TeeDirectoryFactory.NodeLevelTeeDirectoryState();
    this.ioExec = ownState.ioExec;
    Directory naive = new MMapDirectory(path, lockFactory, MMapDirectory.DEFAULT_MAX_CHUNK_SIZE);
    this.access = naive;
    Path compressedPath = path;
    String accessDir = System.getProperty("java.io.tmpdir");
    String pathS = path.toString();
    String scope =
        pathS.endsWith("/index")
            ? TeeDirectoryFactory.getScopeName(pathS)
            : pathS.substring(pathS.lastIndexOf('/'));
    String accessPath = accessDir + scope + "-" + Long.toUnsignedString(System.nanoTime(), 16);
    this.closeLocal =
        () -> {
          try (ownState) {
            PathUtils.delete(Path.of(accessPath));
          }
        };
    accessFunction =
        unused -> {
          Directory dir =
              new AccessDirectory(Path.of(accessPath), lockFactory, compressedPath, ownState);
          return new AbstractMap.SimpleImmutableEntry<>(accessPath, dir);
        };
    persistentFunction =
        content -> {
          assert content == naive;
          content.close();
          content = new CompressingDirectory(compressedPath, ownState.ioExec, true, true);
          return new AbstractMap.SimpleImmutableEntry<>(content, Collections.emptyList());
        };
  }

  public TeeDirectory(
      Directory naive,
      IOFunction<Void, Map.Entry<String, Directory>> accessFunction,
      IOFunction<Directory, Map.Entry<Directory, List<String>>> persistentFunction,
      ExecutorService ioExec) {
    super(TEE_LOCK_FACTORY);
    this.accessFunction = accessFunction;
    this.persistentFunction = persistentFunction;
    this.access = naive;
    this.ioExec = ioExec;
    this.closeLocal = null;
  }

  private List<String> associatedPaths;

  private void init() throws IOException {
    synchronized (persistentFunction) {
      if (this.persistent == null) {
        List<String> buildAssociatedPaths = new ArrayList<>(3);
        Map.Entry<Directory, List<String>> persistentEntry = persistentFunction.apply(access);
        this.persistent = persistentEntry.getKey();
        Path persistentFSPath = ((FSDirectory) persistent).getDirectory();
        buildAssociatedPaths.addAll(persistentEntry.getValue());
        Map.Entry<String, Directory> accessEntry = accessFunction.apply(null);
        this.access = accessEntry.getValue();
        buildAssociatedPaths.add(accessEntry.getKey());
        associatedPaths = buildAssociatedPaths;
      }
    }
  }

  private static final LockFactory TEE_LOCK_FACTORY =
      new LockFactory() {
        @Override
        public Lock obtainLock(Directory dir, String lockName) throws IOException {
          if (!(dir instanceof TeeDirectory)) {
            throw new IllegalArgumentException();
          }
          TeeDirectory teeDir = (TeeDirectory) dir;
          if (IndexWriter.WRITE_LOCK_NAME.equals(lockName)) {
            teeDir.init();
          }
          Lock primary = teeDir.access.obtainLock(lockName);
          if (teeDir.persistent == null) {
            return primary;
          } else {
            Lock secondary;
            try {
              secondary = teeDir.persistent.obtainLock(lockName);
            } catch (Exception e) {
              primary.close();
              throw e;
            }
            return new TeeLock(primary, secondary);
          }
        }
      };

  private static final class TeeLock extends Lock {

    private final Lock primary;
    private final Lock secondary;

    private TeeLock(Lock primary, Lock secondary) {
      this.primary = primary;
      this.secondary = secondary;
    }

    @Override
    public void close() throws IOException {
      try (primary) {
        secondary.close();
      }
    }

    @Override
    public void ensureValid() throws IOException {
      Throwable th = null;
      try {
        secondary.ensureValid();
      } catch (Throwable t) {
        th = t;
      } finally {
        if (th == null) {
          try {
            primary.ensureValid();
          } catch (Throwable t) {
            th = t;
          }
        }
      }
      if (th != null) {
        throw IOUtils.rethrowAlways(th);
      }
    }
  }

  public void removeAssociated() throws IOException {
    synchronized (persistentFunction) {
      if (associatedPaths != null) {
        IOUtils.rm(associatedPaths.stream().map(Path::of).toArray(Path[]::new));
      }
    }
  }

  @Override
  public String[] listAll() throws IOException {
    if (persistent != null) {
      return persistent.listAll();
    } else {
      return access.listAll();
    }
  }

  @Override
  public void deleteFile(String name) throws IOException {
    Throwable th = null;
    try {
      if (persistent != null && !name.endsWith(".tmp")) {
        // persistent directory should never have tmp files; skip files with this reserved
        // extension.
        persistent.deleteFile(name);
      }
    } catch (Throwable t) {
      th = t;
    } finally {
      try {
        access.deleteFile(name);
      } catch (NoSuchFileException ex) {
        // when `persistent != null`, `access` is a special case. Since access may be on ephemeral
        // storage, we should be ok with files being already absent if we're asked to delete them.
        if (persistent == null) {
          th = IOUtils.useOrSuppress(th, ex);
        }
      } catch (Throwable t) {
        th = IOUtils.useOrSuppress(th, t);
      }
    }
    if (th != null) {
      throw IOUtils.rethrowAlways(th);
    }
  }

  @Override
  public long fileLength(String name) throws IOException {
    return access.fileLength(name);
  }

  @Override
  @SuppressWarnings("try")
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    if (name.startsWith("pending_segments_")) {
      init();
    }
    if (persistent == null) {
      return access.createOutput(name, context);
    }
    IndexOutput a = null;
    IndexOutput b = null;
    Throwable th = null;
    try {
      b = persistent.createOutput(name, context);
    } catch (Throwable t) {
      th = t;
    } finally {
      if (b != null) {
        try {
          a = access.createOutput(name, context);
        } catch (Throwable t) {
          try (IndexOutput closeB = b) {
            th = IOUtils.useOrSuppress(th, t);
          } catch (Throwable t1) {
            th = IOUtils.useOrSuppress(th, t1);
          }
        }
      }
    }
    if (th != null) {
      throw IOUtils.rethrowAlways(th);
    }
    assert a != null;
    return new TeeIndexOutput(a, b);
  }

  private static final class TeeIndexOutput extends IndexOutput {
    private final IndexOutput primary;
    private final IndexOutput secondary;

    private TeeIndexOutput(IndexOutput primary, IndexOutput secondary) {
      super("Tee(" + primary.toString() + ", " + secondary.toString() + ")", primary.getName());
      assert primary.getName().equals(secondary.getName());
      this.primary = primary;
      this.secondary = secondary;
    }

    @Override
    public void writeByte(byte b) throws IOException {
      secondary.writeByte(b);
      primary.writeByte(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
      secondary.writeBytes(b, offset, length);
      primary.writeBytes(b, offset, length);
    }

    @Override
    public void close() throws IOException {
      try (primary) {
        secondary.close();
      }
    }

    @Override
    public long getFilePointer() {
      long ret = primary.getFilePointer();
      assert ret == secondary.getFilePointer();
      return ret;
    }

    @Override
    public long getChecksum() throws IOException {
      return primary.getChecksum();
    }
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
      throws IOException {
    return access.createTempOutput(prefix, suffix, context);
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    Future<?> persistentFuture;
    if (persistent == null) {
      persistentFuture = null;
    } else {
      persistentFuture =
          ioExec.submit(
              () -> {
                persistent.sync(names);
                return null;
              });
    }
    Throwable th = null;
    try {
      access.sync(names);
    } catch (Throwable t) {
      th = t;
    } finally {
      if (persistentFuture != null) {
        if (th == null || !persistentFuture.cancel(true)) {
          try {
            persistentFuture.get();
          } catch (InterruptedException e) {
            // we don't throw InterruptedException, so at least we should reset the
            // current thread's interrupt status
            Thread.currentThread().interrupt();
            if (th == null) {
              // make sure this completes exceptionally, but don't add it as
              // a cause, because we've re-interrupted the thread
              th = new RuntimeException("interrupted");
            }
            th.addSuppressed(e);
          } catch (CancellationException e) {
            assert th != null;
            // we are the only ones who could have cancelled this
          } catch (ExecutionException e) {
            th = IOUtils.useOrSuppress(th, e.getCause());
          } catch (Throwable t) {
            th = IOUtils.useOrSuppress(th, t);
          }
        }
      }
    }
    if (th != null) {
      throw IOUtils.rethrowAlways(th);
    }
  }

  @Override
  public void syncMetaData() throws IOException {
    Throwable th = null;
    try {
      if (persistent != null) {
        persistent.syncMetaData();
      }
    } catch (Throwable t) {
      th = t;
    } finally {
      try {
        access.syncMetaData();
      } catch (Throwable t) {
        th = IOUtils.useOrSuppress(th, t);
      }
    }
    if (th != null) {
      throw IOUtils.rethrowAlways(th);
    }
  }

  @Override
  public void rename(String source, String dest) throws IOException {
    Throwable th = null;
    try {
      if (persistent != null) {
        persistent.rename(source, dest);
      }
    } catch (Throwable t) {
      th = t;
    } finally {
      if (th != null) {
        try {
          access.rename(source, dest);
        } catch (Throwable t) {
          th = t;
          if (persistent != null) {
            try {
              // best-effort to put it back, so the operation is atomic across both dirs
              persistent.rename(dest, source);
            } catch (Throwable t1) {
              th = IOUtils.useOrSuppress(th, t1);
            }
          }
        }
      }
    }
    if (th != null) {
      throw IOUtils.rethrowAlways(th);
    }
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    return access.openInput(name, context);
  }

  @Override
  @SuppressWarnings("try")
  public void close() throws IOException {
    try (closeLocal;
        Closeable a = access) {
      if (persistent != null) {
        persistent.close();
      }
    } catch (Exception e) {
      throw IOUtils.rethrowAlways(e);
    }
  }

  @Override
  public Set<String> getPendingDeletions() throws IOException {
    return access.getPendingDeletions();
  }
}
