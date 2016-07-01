package com.latticeengines.datafabric.service.datastore.impl;

import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.datastore.FabricDataStore;

import java.util.List;
import java.util.Set;
import java.util.HashSet;

import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.util.Pool;


@Component("dataService")
public class FabricDataServiceImpl implements FabricDataService {

    private static final Log log = LogFactory.getLog(FabricDataServiceImpl.class);

    @Value("${datafabric.dataService.redisServers}")
    private String redisServers;

    @Value("${datafabric.dataService.redisPort}")
    private int redisPort;

    @Value("${datafabric.dataService.redisActives:128}")
    private int maxActives;

    @Value("${datafabric.dataService.redisHaEnabled:false}")
    private boolean redisHaEnabled;

    @Value("${datafabric.dataService.redisMaster:mymaster}")
    private String redisMaster;

    private Pool<Jedis> jedisPool;

    private boolean initialized;

    public FabricDataServiceImpl() {
    }


    public FabricDataServiceImpl(String master, String redisServers, int redisPort, int maxActives, boolean haEnabled) {
        this.redisMaster = master;
        this.redisServers = redisServers;
        this.redisPort = redisPort;
        this.maxActives = maxActives;
        this.redisHaEnabled = haEnabled;
        this.jedisPool = null;
    }

    synchronized public void init() {
        if (initialized) return;

        log.info("Initialize data service with server " + redisServers + " port " + redisPort);
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(maxActives);

        if (redisHaEnabled) {
            Set<String> sentinelSet = new HashSet<String>();
            String sentinels[] = redisServers.split(",");
            for (String sentinel : sentinels) {
                sentinelSet.add(sentinel + ":" + redisPort);
            }
            jedisPool = new JedisSentinelPool(redisMaster, sentinelSet, poolConfig);
        } else {
            jedisPool = new JedisPool(poolConfig, redisServers, redisPort);
        }
 
        initialized = true;
    }

    public FabricDataStore constructDataStore(String store, String repository, String recordType, Schema schema) {

        log.info("Initialize data store " + store + " repo " + repository);
        if (!initialized) init();

        if (!store.equals("REDIS")) {
            return null;
        }

        RedisDataStoreImpl dataStore = new RedisDataStoreImpl(jedisPool, repository, recordType, schema);
        return dataStore;
    }
}
