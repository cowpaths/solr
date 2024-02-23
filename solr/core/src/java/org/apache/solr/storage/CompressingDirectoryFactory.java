package org.apache.solr.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockFactory;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.StandardDirectoryFactory;

public class CompressingDirectoryFactory extends StandardDirectoryFactory {

  private final ExecutorService ioExec = ExecutorUtil.newMDCAwareCachedThreadPool("ioExec");
  private boolean compress;
  private boolean useAsyncIO;
  private boolean useDirectIO;

  @Override
  public void init(NamedList<?> args) {
    super.init(args);
    SolrParams params = args.toSolrParams();
    compress = params.getBool("compress", true);
    useDirectIO = params.getBool("useDirectIO", CompressingDirectory.DEFAULT_USE_DIRECT_IO);
    useAsyncIO = params.getBool("useAsyncIO", useDirectIO);
  }

  @Override
  protected Directory create(String path, LockFactory lockFactory, DirContext dirContext)
      throws IOException {
    Directory backing;
    Path p = Path.of(path);
    if (compress) {
      backing = new CompressingDirectory(p, ioExec, useAsyncIO, useDirectIO);
    } else {
      backing = FSDirectory.open(p, lockFactory);
    }
    return new SizeAwareDirectory(backing, 0);
  }

  @Override
  @SuppressWarnings("try")
  public void close() throws IOException {
    try (Closeable c = () -> ExecutorUtil.shutdownAndAwaitTermination(ioExec)) {
      super.close();
    }
  }
}
