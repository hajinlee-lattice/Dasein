package com.latticeengines.redis.bean;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.JsonJacksonMapCodec;
import org.redisson.config.Config;
import org.redisson.config.ReplicatedServersConfig;
import org.springframework.beans.factory.FactoryBean;

import com.latticeengines.aws.elasticache.ElastiCacheService;
import com.latticeengines.common.exposed.bean.BeanFactoryEnvironment;

public class RedissonBeanFactory implements FactoryBean<RedissonClient> {

    private ElastiCacheService elastiCacheService;

    @Override
    public RedissonClient getObject() throws Exception {
        BeanFactoryEnvironment.Environment currentEnv = BeanFactoryEnvironment.getEnvironment();
        if (currentEnv == null) {
            throw new IllegalStateException(
                    "BeanFactoryEnvironment has not been initialized yet, check context loading sequence");
        }
        switch (currentEnv) {
            case WebApp:
                return constructClient(3, 0, 3, 0);
            case AppMaster:
                return constructClient(2, 0, 2, 0);
            case TestClient:
            default:
                return constructClient(1, 0, 1, 0);
        }
    }

    @Override
    public Class<?> getObjectType() {
        return RedissonClient.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    private RedissonClient constructClient(int masterPoolSize, int masterIdle, int slavePoolSize, int slaveIdle) {
        Config config = new Config();
        config.setCodec(JsonJacksonMapCodec.INSTANCE);
        ReplicatedServersConfig serverConfig = config.useReplicatedServers()  //
                .setRetryInterval(2500).setScanInterval(2000) //
                .setMasterConnectionPoolSize(masterPoolSize).setMasterConnectionMinimumIdleSize(masterIdle) //
                .setSlaveConnectionPoolSize(slavePoolSize).setSlaveConnectionMinimumIdleSize(slaveIdle);
        elastiCacheService.getNodeAddresses().forEach(serverConfig::addNodeAddress);
        return Redisson.create(config);
    }

    public ElastiCacheService getElastiCacheService() {
        return elastiCacheService;
    }

    public void setElastiCacheService(ElastiCacheService elastiCacheService) {
        this.elastiCacheService = elastiCacheService;
    }
}
