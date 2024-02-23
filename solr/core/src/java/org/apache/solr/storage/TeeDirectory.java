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
            ? TeeDirectoryFactory.getCoreName(pathS)
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
      try {
        secondary.close();
      } finally {
        primary.close();
      }
    }

    @Override
    public void ensureValid() throws IOException {
      try {
        secondary.ensureValid();
      } finally {
        primary.ensureValid();
      }
    }
  }

  public void removeAssociated() throws IOException {
    synchronized (persistentFunction) {
      if (associatedPaths != null) {
        for (String path : associatedPaths) {
          Path dirPath = Path.of(path);
          if (dirPath.toFile().exists()) {
            PathUtils.deleteDirectory(dirPath);
          }
        }
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
    try {
      if (persistent != null && !name.endsWith(".tmp")) {
        // persistent directory should never have tmp files; skip files with this reserved
        // extension.
        persistent.deleteFile(name);
      }
    } finally {
      try {
        access.deleteFile(name);
      } catch (NoSuchFileException ex) {
        // when `persistent != null`, `access` is a special case. Since access may be on ephemeral
        // storage, we should be ok with files being already absent if we're asked to delete them.
        if (persistent == null) {
          throw ex;
        }
      }
    }
  }

  @Override
  public long fileLength(String name) throws IOException {
    return access.fileLength(name);
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    if (name.startsWith("pending_segments_")) {
      init();
    }
    if (persistent == null) {
      return access.createOutput(name, context);
    }
    IndexOutput a;
    IndexOutput b;
    try {
      b = persistent.createOutput(name, context);
    } finally {
      a = access.createOutput(name, context);
    }
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
    boolean success = false;
    try {
      access.sync(names);
      success = true;
    } finally {
      if (persistentFuture != null) {
        if (success || !persistentFuture.cancel(true)) {
          try {
            persistentFuture.get();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
              throw (IOException) cause;
            }
            throw new RuntimeException(e);
          }
        }
      }
    }
  }

  @Override
  public void syncMetaData() throws IOException {
    try {
      if (persistent != null) {
        persistent.syncMetaData();
      }
    } finally {
      access.syncMetaData();
    }
  }

  @Override
  public void rename(String source, String dest) throws IOException {
    try {
      if (persistent != null) {
        persistent.rename(source, dest);
      }
    } finally {
      access.rename(source, dest);
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
      throw new RuntimeException(e);
    }
  }

  @Override
  public Set<String> getPendingDeletions() throws IOException {
    return access.getPendingDeletions();
  }
}
