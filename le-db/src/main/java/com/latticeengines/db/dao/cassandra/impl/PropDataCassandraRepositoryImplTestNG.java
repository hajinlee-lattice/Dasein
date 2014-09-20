package com.latticeengines.db.dao.cassandra.impl;

import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.db.dao.cassandra.CassandraGenericDao;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dataplatform-context.xml" })
public class PropDataCassandraRepositoryImplTestNG extends
		AbstractTestNGSpringContextTests {

	@Resource(name = "propDataCassandraGenericDao")
	private CassandraGenericDao dao;

	@Test(groups = "functional")
	public void queryForListOfMapByTableColumn() {
		List<Map<String, Object>> result = dao
				.queryForListOfMapByTableColumn("\"HGData_Source\"", "uuid",
						"1F5AA348-D87E-48D8-999D-92708BB8DD0A");
		Assert.assertEquals(result.size(), 1);
		Assert.assertEquals(result.get(0).get("Url"), "emdiesels.com");
		Assert.assertEquals(result.get(0).get("Supplier Name"), "Salesforce.com");

	}
}
