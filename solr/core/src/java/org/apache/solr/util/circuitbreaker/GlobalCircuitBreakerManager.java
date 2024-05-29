package org.apache.solr.util.circuitbreaker;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.cloud.ClusterPropertiesListener;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.SolrJacksonAnnotationInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GlobalCircuitBreakerManager implements ClusterPropertiesListener {
    protected static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final ObjectMapper mapper = SolrJacksonAnnotationInspector.createObjectMapper();
    private CircuitBreakerRegistry cbRegistry;
    public CircuitBreakerRegistry getCircuitBreakerRegistry() {
        return cbRegistry;
    }

    private GlobalCircuitBreakerConfig currentConfig;

    public GlobalCircuitBreakerManager() {
        super();
        this.cbRegistry = new CircuitBreakerRegistry();
    }

    private static class GlobalCircuitBreakerConfig {
        static final String CIRCUIT_BREAKER_CLUSTER_PROPS_KEY = "circuit-breakers";
        @JsonProperty
        CircuitBreakerConfig load;

        static class CircuitBreakerConfig {
            @JsonProperty Boolean enabled;
            @JsonProperty Long updateThreshold;
            @JsonProperty Long queryThreshold;
        }

        @Override
        public String toString() {
            return String.format("enabled=%s;updateThreshold=%s;queryThreshold=%s;", load.enabled, load.updateThreshold, load.queryThreshold);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof GlobalCircuitBreakerConfig)) {
                return false;
            }
            GlobalCircuitBreakerConfig that = (GlobalCircuitBreakerConfig) obj;
            return this.load.enabled == that.load.enabled &&
                    this.load.updateThreshold.equals(that.load.updateThreshold) &&
                    this.load.queryThreshold.equals(that.load.queryThreshold);
        }

        @Override
        public int hashCode() {
            return Objects.hash(load.enabled, load.queryThreshold, load.updateThreshold);
        }
    }

    // for registering global circuit breakers set in clusterprops
    @Override
    public boolean onChange(Map<String, Object> properties) {
        try {
            GlobalCircuitBreakerConfig nextConfig = processConfigChange(properties);
            if (nextConfig != null) {
                log.info(nextConfig.toString());
                if (this.currentConfig == null || !this.currentConfig.equals(nextConfig)) {
                    registerCircuitBreakers(nextConfig);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return false;
    }

    private GlobalCircuitBreakerConfig processConfigChange(Map<String, Object> properties) throws IOException {
        Object cbConfig = properties.get(GlobalCircuitBreakerConfig.CIRCUIT_BREAKER_CLUSTER_PROPS_KEY);
        GlobalCircuitBreakerConfig globalCBConfig = null;
        if (cbConfig != null) {
            log.info(cbConfig.toString());
            byte[] configInput = Utils.toJSON(properties.get(GlobalCircuitBreakerConfig.CIRCUIT_BREAKER_CLUSTER_PROPS_KEY));
            if (configInput != null && configInput.length > 0) {
                globalCBConfig = mapper.readValue(configInput, GlobalCircuitBreakerConfig.class);
            }
        }
        return globalCBConfig;
    }

    private void registerCircuitBreakers(GlobalCircuitBreakerConfig gbConfig) throws IOException {
        this.currentConfig = gbConfig;
        log.info("registering config!");
        this.cbRegistry.deregisterAll();

        if (gbConfig.load.enabled) {
            if (gbConfig.load.queryThreshold != null) {
                LoadAverageCircuitBreaker newQueryLoadCB = new LoadAverageCircuitBreaker();
                newQueryLoadCB.setThreshold(gbConfig.load.queryThreshold);
                newQueryLoadCB.setRequestTypes(List.of(SolrRequest.SolrRequestType.QUERY.name()));
                this.cbRegistry.register(newQueryLoadCB);
            }
            if (gbConfig.load.updateThreshold != null) {
                LoadAverageCircuitBreaker newUpdateLoadCb = new LoadAverageCircuitBreaker();
                newUpdateLoadCb.setThreshold(gbConfig.load.updateThreshold);
                newUpdateLoadCb.setRequestTypes(List.of(SolrRequest.SolrRequestType.UPDATE.name()));
                this.cbRegistry.register(newUpdateLoadCb);
            }
        }
        log.info("registered cbs! {}", this.cbRegistry.circuitBreakerMap);
    }
}
