package com.latticeengines.datafabric.service.datastore.impl;

import java.util.HashSet;
import java.util.Set;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.datafabric.service.datastore.FabricDataServiceProvider;
import com.latticeengines.datafabric.service.datastore.FabricDataStore;
import com.latticeengines.domain.exposed.datafabric.FabricStoreEnum;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.util.Pool;

@Component("redisDataService")
public class RedisDataServiceProvider implements FabricDataServiceProvider {

    private static final Logger log = LoggerFactory.getLogger(FabricDataServiceImpl.class);

    @Value("${datafabric.dataService.redis.servers:localhost}")
    private String redisServers;

    @Value("${datafabric.dataService.redis.port:2181}")
    private int redisPort;

    @Value("${datafabric.dataService.redis.actives:128}")
    private int maxActives;

    @Value("${datafabric.dataService.redis.ha.enabled:false}")
    private boolean redisHaEnabled;

    @Value("${datafabric.dataService.redis.master:mymaster}")
    private String redisMaster;

    private Pool<Jedis> jedisPool;

    private boolean initialized;

    public RedisDataServiceProvider() {
    }

    public RedisDataServiceProvider(String master, String redisServers, int redisPort, int maxActives,
            boolean haEnabled) {
        this.redisMaster = master;
        this.redisServers = redisServers;
        this.redisPort = redisPort;
        this.maxActives = maxActives;
        this.redisHaEnabled = haEnabled;
        this.jedisPool = null;
    }

    synchronized private void init() {
        if (initialized)
            return;

        log.info("Initialize data service with server " + redisServers + " port " + redisPort);
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(maxActives);

        if (redisHaEnabled) {
            Set<String> sentinelSet = new HashSet<String>();
            String sentinels[] = redisServers.split(",");
            for (String sentinel : sentinels) {
                String hap = sentinel + ":" + redisPort;
                sentinelSet.add(hap);
                log.info("Add " + hap + " to sentinelSet");
            }
            jedisPool = new JedisSentinelPool(redisMaster, sentinelSet, poolConfig);
        } else {
            jedisPool = new JedisPool(poolConfig, redisServers, redisPort);
        }

        initialized = true;
    }

    public FabricDataStore constructDataStore(String repository, String recordType, Schema schema) {

        FabricDataStore dataStore = null;
        log.info("Initialize Dynamo data store " + repository + " " + recordType);
        if (!initialized)
            init();

        dataStore = new RedisDataStoreImpl(jedisPool, repository, recordType, schema);
        return dataStore;
    }

    public String getName() {
        return FabricStoreEnum.REDIS.name();
    }
}
