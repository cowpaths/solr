package org.apache.solr.core;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.rest.RestManager;

public class SyntheticSolrCore extends SolrCore {
  public SyntheticSolrCore(CoreContainer coreContainer, CoreDescriptor cd, ConfigSet configSet) {
    super(coreContainer, cd, configSet);
  }

  public static SyntheticSolrCore createAndRegisterCore(
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
    SyntheticSolrCore syntheticCore =
        new SyntheticSolrCore(coreContainer, syntheticCoreDescriptor, coreConfig);
    coreContainer.registerCore(syntheticCoreDescriptor, syntheticCore, false, false);

    return syntheticCore;
  }

  @Override
  protected void bufferUpdatesIfConstructing(CoreDescriptor coreDescriptor) {
    //no updates to SyntheticSolrCore
  }

  @Override
  protected RestManager initRestManager() throws SolrException {
    return new RestManager(); //TODO explain why we cannot use the super class init routine
  }
}
