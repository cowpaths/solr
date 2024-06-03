package org.apache.solr.util.circuitbreaker;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.cloud.ClusterPropertiesListener;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.SolrJacksonAnnotationInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GlobalCircuitBreakerManager implements ClusterPropertiesListener {
    protected static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final ObjectMapper mapper = SolrJacksonAnnotationInspector.createObjectMapper();
    private final CircuitBreakerRegistry cbRegistry = new CircuitBreakerRegistry();
    private final CoreContainer coreContainer;

    public CircuitBreakerRegistry getCircuitBreakerRegistry() {
        return cbRegistry;
    }

    private GlobalCircuitBreakerConfig currentConfig = new GlobalCircuitBreakerConfig();;

    public GlobalCircuitBreakerManager(CoreContainer coreContainer) {
        super();
        this.coreContainer = coreContainer;
    }

    private static class GlobalCircuitBreakerConfig {
        static final String CIRCUIT_BREAKER_CLUSTER_PROPS_KEY = "circuit-breakers";
        @JsonProperty
        CircuitBreakerConfig load = new CircuitBreakerConfig();
        @JsonProperty
        CircuitBreakerConfig cpu = new CircuitBreakerConfig();
        @JsonProperty
        CircuitBreakerConfig memory = new CircuitBreakerConfig();

        static class CircuitBreakerConfig {
            @JsonProperty Boolean enabled = false;
            @JsonProperty Long updateThreshold = Long.MAX_VALUE;
            @JsonProperty Long queryThreshold = Long.MAX_VALUE;

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
            return this.load.equals(that.load) && this.cpu.equals(that.cpu) && this.memory.equals(that.memory);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.load.hashCode(), this.cpu.hashCode(), this.memory.hashCode());
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
        } catch (IOException e) {
            log.warn("error parsing global circuit breaker configuration {}", e.getMessage());
            // don't break when things are misconfigured
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
        this.cbRegistry.deregisterAll();
        if (gbConfig.load.enabled) {
            if (gbConfig.load.queryThreshold != null) {
                registerGlobalCircuitBreaker(new LoadAverageCircuitBreaker(), gbConfig.load.queryThreshold, SolrRequest.SolrRequestType.QUERY);
            }
            if (gbConfig.load.updateThreshold != null) {
                registerGlobalCircuitBreaker(new LoadAverageCircuitBreaker(), gbConfig.load.updateThreshold, SolrRequest.SolrRequestType.UPDATE);
            }
        }
        if (gbConfig.cpu.enabled) {
            if (gbConfig.cpu.queryThreshold != null) {
                registerGlobalCircuitBreaker(new CPUCircuitBreaker(coreContainer), gbConfig.cpu.queryThreshold, SolrRequest.SolrRequestType.QUERY);
            }
            if (gbConfig.cpu.updateThreshold != null) {
                registerGlobalCircuitBreaker(new CPUCircuitBreaker(coreContainer), gbConfig.cpu.updateThreshold, SolrRequest.SolrRequestType.UPDATE);
            }
        }
        if (gbConfig.memory.enabled) {
            if (gbConfig.memory.queryThreshold != null) {
                registerGlobalCircuitBreaker(new MemoryCircuitBreaker(), gbConfig.memory.queryThreshold, SolrRequest.SolrRequestType.QUERY);
            }
            if (gbConfig.memory.updateThreshold != null) {
                registerGlobalCircuitBreaker(new MemoryCircuitBreaker(), gbConfig.memory.updateThreshold, SolrRequest.SolrRequestType.UPDATE);
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
