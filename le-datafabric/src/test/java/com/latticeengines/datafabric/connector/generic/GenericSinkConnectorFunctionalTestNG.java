package com.latticeengines.datafabric.connector.generic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.datafabric.entitymanager.GenericFabricMessageManager;
import com.latticeengines.datafabric.entitymanager.impl.SampleEntity;
import com.latticeengines.datafabric.functionalframework.DataFabricFunctionalTestNGBase;

public class GenericSinkConnectorFunctionalTestNG extends DataFabricFunctionalTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(GenericSinkConnectorFunctionalTestNG.class);

    @Value("datafabric.message.stack")
    private String stack;

    @Value("datafabric.message.zkConnect")
    private String zkConnect;

    @Resource(name = "genericFabricMessageManager")
    private GenericFabricMessageManager<SampleEntity> entityManager;

    private GenericSinkConnector connector;

    @BeforeMethod(groups = "functional")
    public void setUp() throws Exception {

        connector = new GenericSinkConnector();
        Map<String, String> props = new HashMap<>();
        props.put(GenericSinkConnectorConfig.STACK.getKey(), stack);
        props.put(GenericSinkConnectorConfig.KAFKA_ZKCONNECT.getKey(), zkConnect);
        props.put(GenericSinkConnectorConfig.HDFS_BASE_DIR.getKey(), BASE_DIR);

        connector.start(props);
    }

    @AfterMethod(groups = "functional")
    public void tearDown() throws Exception {
    }

    @Test(groups = "functional", enabled = true)
    public void put() throws Exception {
        Map<String, String> properties = connector.configProperties;
        Assert.assertEquals(properties.get(GenericSinkConnectorConfig.STACK.getKey()), stack);
        Assert.assertEquals(properties.get(GenericSinkConnectorConfig.KAFKA_ZKCONNECT.getKey()), zkConnect);
        Assert.assertEquals(properties.get(GenericSinkConnectorConfig.HDFS_BASE_DIR.getKey()), BASE_DIR);
    }

    @Test(groups = "functional", enabled = true)
    public void taskConfigs() throws Exception {
        List<Map<String, String>> properties = connector.taskConfigs(2);
        Assert.assertEquals(properties.size(), 2);
        Assert.assertEquals(properties.get(0).get(GenericSinkConnectorConfig.STACK.getKey()), stack);
        Assert.assertEquals(properties.get(1).get(GenericSinkConnectorConfig.STACK.getKey()), stack);
    }

}
