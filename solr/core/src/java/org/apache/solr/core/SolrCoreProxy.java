package org.apache.solr.core;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.solr.common.params.CoreAdminParams;

public class SolrCoreProxy extends SolrCore {
  public SolrCoreProxy(CoreContainer coreContainer, CoreDescriptor cd, ConfigSet configSet) {
    super(coreContainer, cd, configSet, true);
  }

  public static SolrCoreProxy createAndRegisterProxy(
      CoreContainer coreContainer, String syntheticCollectionName, String configSetName) {
    Map<String, String> coreProps = new HashMap<>();
    coreProps.put(CoreAdminParams.CORE_NODE_NAME, coreContainer.getHostName());
    coreProps.put(CoreAdminParams.COLLECTION, syntheticCollectionName);

    CoreDescriptor syntheticCoreDescriptor =
        new CoreDescriptor(
            syntheticCollectionName,
            Paths.get(coreContainer.getSolrHome() + "/" + syntheticCollectionName),
            coreProps,
            coreContainer.getContainerProperties(),
            coreContainer.getZkController());

    ConfigSet coreConfig =
        coreContainer.getConfigSetService().loadConfigSet(syntheticCoreDescriptor, configSetName);
    syntheticCoreDescriptor.setConfigSetTrusted(coreConfig.isTrusted());
    SolrCoreProxy syntheticCore =
        new SolrCoreProxy(coreContainer, syntheticCoreDescriptor, coreConfig);
    coreContainer.registerCore(syntheticCoreDescriptor, syntheticCore, false, false);

    return syntheticCore;
  }
}
