package com.latticeengines.redis.bean;

import java.util.List;

import org.springframework.data.redis.connection.RedisStandaloneConfiguration;

import io.lettuce.core.RedisURI;

public class LedpMasterSlaveConfiguration extends RedisStandaloneConfiguration {

    private final List<RedisURI> endpoints;

    public LedpMasterSlaveConfiguration(List<RedisURI> endpoints) {
        this.endpoints = endpoints;
    }

    public List<RedisURI> getEndpoints() {
        return endpoints;
    }
}
