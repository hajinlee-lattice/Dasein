package com.latticeengines.cassandra.dao.impl;

import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.cassandra.exposed.dao.CassandraGenericDao;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:cassandra-context.xml", "classpath:cassandra-properties-context.xml" })
public class PropDataCassandraRepositoryImplTestNG extends AbstractTestNGSpringContextTests {

    @Resource(name = "propDataCassandraGenericDao")
    private CassandraGenericDao dao;

    @Test(groups = "manual")
    public void queryForListOfMapByTableColumn() {
        List<Map<String, Object>> result = dao.queryForListOfMapByTableColumn("\"HGData_Source\"", "uuid",
                "1F5AA348-D87E-48D8-999D-92708BB8DD0A", 10);
        Assert.assertEquals(result.size(), 1);
        Assert.assertEquals(result.get(0).get("Url"), "emdiesels.com");
        Assert.assertEquals(result.get(0).get("Supplier Name"), "Salesforce.com");

    }

    @Test(groups = "manual")
    public void findByKey() {
        Map<String, Object> result = dao.findByKey("\"HGData_Source\"", "uuid", "1F5AA348-D87E-48D8-999D-92708BB8DD0A");
        Assert.assertEquals(result.get("Url"), "emdiesels.com");

    }
}
