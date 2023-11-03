package org.apache.solr.cloud;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.function.Consumer;

/**
 * <p>A simple alternative to {@link org.apache.solr.common.cloud.ZkStateReader#CLUSTER_PROPS}
 * (single json file to contain all properties).
 *
 * <p>This allows one znode per property for ease of concurrency management and update (no need to parse json)
 * and more precise listener logic.
 *
 * <p>Currently, this only supports 1 listener per instance (ie multiple watches translate to multiple instances
 * and ZK connections/watches)
 */
public class ZkDynamicProperty implements Watcher {
  private final SolrZkClient zkClient;
  private final Consumer<byte[]> listener;
  public static final String DYNAMIC_PROPERTIES_ZKNODE = "/dynamic_properties";
  private final String path;

  /**
   * Listens to a dynamic property on Zk. If such property is not yet defined, an empty znode will be created
   * @param propertyName  property name to be created. Take note that this is also uses as part of the final znode path
   *                      so / could be used for nested structure
   */
  public ZkDynamicProperty(SolrZkClient zkClient, String propertyName, Consumer<byte[]> listener) {
    this.zkClient = zkClient;
    this.listener = listener;
    this.path = DYNAMIC_PROPERTIES_ZKNODE + "/" + propertyName;
    //TODO validate propertyName?

    try {
      if (!zkClient.exists(path, true)) {
        zkClient.makePath(path, new byte[0], CreateMode.PERSISTENT, true);
      }
      byte[] data = zkClient.getData(path, this, null, true);
      listener.accept(data);
    } catch (KeeperException | InterruptedException e) {
      throw new SolrException(
              SolrException.ErrorCode.SERVER_ERROR,
              "Error initializing dynamic_property with path [" + path + "]",
              e);
    }
  }


  @Override
  public void process(WatchedEvent event) {
    switch (event.getType()) {
      case NodeDataChanged:
        try {
          byte[] data = zkClient.getData(path, this, null, true);
          listener.accept(data);
        } catch (KeeperException | InterruptedException e) {
          throw new SolrException(
                  SolrException.ErrorCode.SERVER_ERROR,
                  "Error fetch data from zk path [" + path + "]",
                  e);
        }
        break;
      case NodeDeleted:
        listener.accept(null);
        break;
    }
  }
}
