package org.apache.solr.util.circuitbreaker;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.cloud.ClusterPropertiesListener;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.SolrJacksonAnnotationInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class GlobalCircuitBreakerManager implements ClusterPropertiesListener {
    protected static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final ObjectMapper mapper = SolrJacksonAnnotationInspector.createObjectMapper();
    private final CircuitBreakerRegistry cbRegistry = new CircuitBreakerRegistry();
    private final GlobalCircuitBreakerFactory factory;

    public CircuitBreakerRegistry getCircuitBreakerRegistry() {
        return cbRegistry;
    }

    private GlobalCircuitBreakerConfig currentConfig = new GlobalCircuitBreakerConfig();;

    public GlobalCircuitBreakerManager(CoreContainer coreContainer) {
        super();
        this.factory = new GlobalCircuitBreakerFactory(coreContainer);
    }

    private static class GlobalCircuitBreakerConfig {
        static final String CIRCUIT_BREAKER_CLUSTER_PROPS_KEY = "circuit-breakers";
        @JsonProperty
        Map<String, CircuitBreakerConfig> configs = new ConcurrentHashMap<>();

        static class CircuitBreakerConfig {
            @JsonProperty Boolean enabled = false;
            @JsonProperty Double updateThreshold = Double.MAX_VALUE;
            @JsonProperty Double queryThreshold = Double.MAX_VALUE;

            @Override
            public boolean equals(Object obj) {
                if (!(obj instanceof CircuitBreakerConfig)) {
                    return false;
                }
                CircuitBreakerConfig that = (CircuitBreakerConfig) obj;
                return this.enabled == that.enabled &&
                        this.updateThreshold.equals(that.updateThreshold) &&
                        this.queryThreshold.equals(that.queryThreshold);
            }

            @Override
            public int hashCode() {
                return Objects.hash(this.enabled, this.queryThreshold, this.updateThreshold);
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof GlobalCircuitBreakerConfig)) {
                return false;
            }
            GlobalCircuitBreakerConfig that = (GlobalCircuitBreakerConfig) obj;
            if (that.configs.size() != this.configs.size()) {
                return false;
            }
            for (Map.Entry<String, CircuitBreakerConfig> entry : configs.entrySet()) {
                CircuitBreakerConfig thisConfig = entry.getValue();
                CircuitBreakerConfig thatConfig = that.configs.get(entry.getKey());
                if (thatConfig == null) {
                    return false;
                }
                if (!thisConfig.equals(thatConfig)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            // Map.Entry already overrides hashCode, so let's use that
            return Objects.hash(configs.entrySet());
        }
    }

    // for registering global circuit breakers set in clusterprops
    @Override
    public boolean onChange(Map<String, Object> properties) {
        try {
            GlobalCircuitBreakerConfig nextConfig = processConfigChange(properties);
            if (nextConfig != null) {
                if (!this.currentConfig.equals(nextConfig)) {
                    registerCircuitBreakers(nextConfig);
                }
            }
        } catch (Exception e) {
            // don't break when things are misconfigured
            log.warn("error parsing global circuit breaker configuration {}", e.getMessage());
        }
        return false;
    }

    private GlobalCircuitBreakerConfig processConfigChange(Map<String, Object> properties) throws IOException {
        Object cbConfig = properties.get(GlobalCircuitBreakerConfig.CIRCUIT_BREAKER_CLUSTER_PROPS_KEY);
        GlobalCircuitBreakerConfig globalCBConfig = null;
        if (cbConfig != null) {
            byte[] configInput = Utils.toJSON(properties.get(GlobalCircuitBreakerConfig.CIRCUIT_BREAKER_CLUSTER_PROPS_KEY));
            if (configInput != null && configInput.length > 0) {
                globalCBConfig = mapper.readValue(configInput, GlobalCircuitBreakerConfig.class);
            }
        }
        return globalCBConfig;
    }

    private void registerCircuitBreakers(GlobalCircuitBreakerConfig gbConfig) throws Exception {
        this.currentConfig = gbConfig;
        this.cbRegistry.deregisterAll();
        for (Map.Entry<String, GlobalCircuitBreakerConfig.CircuitBreakerConfig> entry : this.currentConfig.configs.entrySet()) {
            GlobalCircuitBreakerConfig.CircuitBreakerConfig config = entry.getValue();
            try {
                if (config.enabled) {
                    if (config.queryThreshold != Double.MAX_VALUE) {
                        registerGlobalCircuitBreaker(this.factory.create(entry.getKey()), config.queryThreshold, SolrRequest.SolrRequestType.QUERY);
                    }
                    if (config.updateThreshold != Double.MAX_VALUE) {
                        registerGlobalCircuitBreaker(this.factory.create(entry.getKey()), config.updateThreshold, SolrRequest.SolrRequestType.UPDATE);
                    }
                }
            } catch (Exception e) {
                log.warn("error while registering global circuit breaker {}: {}", entry.getKey(), e.getMessage());
            }
        }
        log.info("onChange registered circuit breakers {}", this.cbRegistry.circuitBreakerMap);
    }

    private void registerGlobalCircuitBreaker(CircuitBreaker globalCb, double threshold, SolrRequest.SolrRequestType type) {
        globalCb.setThreshold(threshold);
        globalCb.setRequestTypes(List.of(type.name()));
        this.cbRegistry.register(globalCb);
    }
}
