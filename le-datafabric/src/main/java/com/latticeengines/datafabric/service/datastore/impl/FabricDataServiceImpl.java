package com.latticeengines.datafabric.service.datastore.impl;

import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.datastore.FabricDataStore;

import javax.annotation.PostConstruct;

import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;


@Component("dataService")
public class FabricDataServiceImpl implements FabricDataService {

    private static final Log log = LogFactory.getLog(FabricDataServiceImpl.class);

    @Value("${datafabric.dataService.redisServers}")
    private String redisServers;

    @Value("${datafabric.dataService.redisPort}")
    private int redisPort;

    @Value("${datafabric.dataService.redisActives:128}")
    private int maxActives;

    private JedisPool jedisPool;

    private boolean initialized = false;

    public FabricDataServiceImpl() {
    }


    public FabricDataServiceImpl(String redisServers, int redisPort, int maxActives) {
        this.redisServers = redisServers;
        this.redisPort = redisPort;
        this.maxActives = maxActives;
    }

    @PostConstruct
    public void init() {

        if (initialized) return;

        log.info("Initialize data service with server " + redisServers + " port " + redisPort);
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(maxActives);
        jedisPool = new JedisPool(poolConfig, redisServers, redisPort);
        initialized = true;
    }

    public FabricDataStore constructDataStore(String store, String repository, String recordType, Schema schema) {

        log.info("Initialize data store " + store + " repo " + repository);
        if (!store.equals("REDIS")) {
            return null;
        }

        RedisDataStoreImpl dataStore = new RedisDataStoreImpl(jedisPool, repository, recordType, schema);
        return dataStore;
    }
}
