package org.apache.solr.cloud;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import org.apache.solr.common.cloud.ClusterPropertiesListener;

public class WatchedClusterProperties implements ClusterPropertiesListener {

  public static final String WATCHED_PROPERTIES = "watched-properties";

  private Map<String, List<BiConsumer<String, String>>> propertiesListeners =
      Collections.synchronizedMap(new HashMap<>());

  private volatile Map<String, String> knownData = new HashMap<>();

  @Override
  @SuppressWarnings("unchecked")
  public boolean onChange(Map<String, Object> properties) {
    Map<String, String> newData = (Map<String, String>) properties.get(WATCHED_PROPERTIES);
    if (newData == null) newData = Collections.EMPTY_MAP;
    compareAndInvokeListeners(newData);
    return false;
  }

  private void compareAndInvokeListeners(Map<String, String> newData) {
    boolean isModified = false;
    // look for changed data or missing keys
    for (String k : knownData.keySet()) {
      String oldVal = knownData.get(k);
      String newVal = newData.get(k);
      if (Objects.equals(oldVal, newVal)) continue;
      isModified = true;
      invokeListener(k,  newVal);
    }
    for (String k : newData.keySet()) {
      if (knownData.containsKey(k)) continue;
      isModified = true;
      invokeListener(k,  newData.get(k));
    }

    if (isModified) {
      knownData = Map.copyOf(newData);
    }
  }

  private void invokeListener(String key, String newVal) {
    List<BiConsumer<String, String>> listeners = propertiesListeners.get(key);
    if (listeners != null) {
      for (BiConsumer<String, String> l : listeners) {
        l.accept(key, newVal);
      }
    }
    listeners = propertiesListeners.get(null);
    if (listeners != null) {
      for (BiConsumer<String, String> listener : listeners) {
        listener.accept(key, newVal);
      }
    }
  }

  public void watchProperty(String name, BiConsumer<String, String> listener) {
    propertiesListeners.computeIfAbsent(name, s -> new CopyOnWriteArrayList<>()).add(listener);
  }

  public void unwatchProperty(String name, BiConsumer<String, String> listener) {
    List<BiConsumer<String, String>> listeners = propertiesListeners.get(name);
    if (listeners == null) return;
    listeners.remove(listener);
  }
}
