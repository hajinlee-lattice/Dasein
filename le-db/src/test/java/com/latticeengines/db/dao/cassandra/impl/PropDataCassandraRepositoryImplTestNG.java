package com.latticeengines.db.dao.cassandra.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import javax.annotation.Resource;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.latticeengines.db.dao.cassandra.CassandraGenericDao;
import com.latticeengines.db.dao.cassandra.data.Column;
import com.latticeengines.db.dao.cassandra.data.ColumnType;
import com.latticeengines.db.dao.cassandra.data.ColumnTypeMapper;
import com.latticeengines.db.dao.cassandra.data.Table;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:db-context.xml",
		"classpath:db-properties-context.xml" })
public class PropDataCassandraRepositoryImplTestNG extends
		AbstractTestNGSpringContextTests {

	@Resource(name = "propDataCassandraGenericDao")
	private CassandraGenericDao dao;

	@Resource(name = "poolService")
	private ExecutorService pool;

	@Test(groups = "manual")
	public void queryForListOfMapByTableColumn() {
		List<Map<String, Object>> result = dao.queryForListOfMapByTableColumn(
				"\"HGData_Source\"", "uuid",
				"1F5AA348-D87E-48D8-999D-92708BB8DD0A", 10);
		Assert.assertEquals(result.size(), 1);
		Assert.assertEquals(result.get(0).get("Url"), "emdiesels.com");
		Assert.assertEquals(result.get(0).get("Supplier Name"),
				"Salesforce.com");

	}

	@Test(groups = "manual")
	public void findByKey() {
		Map<String, Object> result = dao.findByKey("\"HGData_Source\"", "uuid",
				"1F5AA348-D87E-48D8-999D-92708BB8DD0A");
		Assert.assertEquals(result.get("Url"), "emdiesels.com");

	}

	@Test(groups = "manual")
	public void createDomainIndex() {

		String domainIndexTable = "Domain_Index";
		dao.dropTable(domainIndexTable);

		Table table = Table.newInstance(domainIndexTable);
		table.addPartitionColumn(new Column("domain_name", ColumnType.VARCHAR));
		dao.createTable(table);

		String limit = 999_999_999 + "";
		indexTable(domainIndexTable, "HGData_Source", "Url", "uuid",
				"domain_name", limit);
		indexTable(domainIndexTable, "BuiltWithDomain", "Domain", "uuid",
				"domain_name", limit);
		indexTable(domainIndexTable, "Experian_Source", "URL", "uuid",
				"domain_name", limit);
	}

	@Test(groups = "manual")
	public void lookupIndex() {
		long startTime = System.currentTimeMillis();
		String domainIndexTable = "Domain_Index";
		dao.createIndex(domainIndexTable, "HGData_Source",
				"HGData_Source_Index");
		dao.createIndex(domainIndexTable, "BuiltWithDomain",
				"BuiltWithDomain_Index");
		dao.createIndex(domainIndexTable, "Experian_Source",
				"Experian_Source_Index");

		String cql = "SELECT * FROM " + domainIndexTable;
		ResultSet resultSet = dao.query(cql);
		Row row = null;

		int totalCount = 0;
		int oneSourceCount = 0;
		int twoSourceCount = 0;
		int threeSourceCount = 0;

		while ((row = resultSet.one()) != null) {
			Map<String, Object> map = ColumnTypeMapper.convertRowToMap(row);
			String hgDataUUID = (String) map.get("hgdata_source");
			String builtWithUUID = (String) map.get("builtwithdomain");
			String experianUUID = (String) map.get("experian_source");

			int sourceCount = 0;
			
			if (hgDataUUID != null) {
				Assert.assertNotNull(dao.findByKey2("\"HGData_Source\"",
						"uuid", hgDataUUID));
				sourceCount++;
			}
			
			if (builtWithUUID != null) {
				Assert.assertNotNull(dao.findByKey2("\"BuiltWithDomain\"",
						"uuid", builtWithUUID));
				sourceCount++;
			}
			
			if (experianUUID != null) {
				Assert.assertNotNull(dao.findByKey2("\"Experian_Source\"",
						"uuid", experianUUID));
				sourceCount++;
			}

			if (sourceCount == 1) {
				oneSourceCount++;
			} else if (sourceCount == 2) {
				twoSourceCount++;
			} else if (sourceCount == 3) {
				threeSourceCount++;
			}
			
			totalCount++;
			
			if (totalCount % 1000 == 0) {
				System.out.println("Count=" + totalCount);
			}
		}
		System.out.println("One source count=" + oneSourceCount);
		System.out.println("Two source count=" + twoSourceCount);
		System.out.println("Three source count=" + threeSourceCount);
		System.out.println("Total source count=" + totalCount);
		System.out
				.println("Index lookup total time="
						+ (System.currentTimeMillis() - startTime) / 1000
						+ " seconds.");
	}

	private void indexTable(String domainIndexTable, String sourceTableName,
			String sourceDomainColumn, String sourceKeyColumn,
			String indexKeyColumn, String limit) {

		dao.addColumn(domainIndexTable, new Column(sourceTableName,
				ColumnType.VARCHAR));
		String cql = "SELECT " + sourceKeyColumn + ", \"" + sourceDomainColumn
				+ "\" FROM \"" + sourceTableName + "\" LIMIT " + limit;

		ResultSet resultSet = dao.query(cql);
		Row row = null;
		int countWithDomain = 0;
		int countWithoutDomain = 0;
		long startTime = System.currentTimeMillis();

		while ((row = resultSet.one()) != null) {
			Map<String, Object> map = ColumnTypeMapper.convertRowToMap(row);
			Map<String, Object> indexMap = getIndexMapFromSourceMap(map,
					sourceDomainColumn, sourceKeyColumn, indexKeyColumn,
					sourceTableName);
			if (indexMap == null) {
				countWithoutDomain++;
				continue;
			}
			countWithDomain++;

			submitTask(domainIndexTable, indexMap);
		}

		System.out.println("Table=" + sourceTableName
				+ ", total row count for rows with domain is="
				+ countWithDomain);
		System.out.println("Table=" + sourceTableName
				+ ", total row count for rows WITHOUT domain is="
				+ countWithoutDomain);
		System.out
				.println("Table=" + sourceTableName + ", total time="
						+ (System.currentTimeMillis() - startTime) / 1000
						+ " seconds.");
	}

	private void submitTask(String domainIndexTable,
			Map<String, Object> indexMap) {

		// dao.save(table, map);

		IndexTask<Boolean> task = new IndexTask<>(domainIndexTable, indexMap);
		pool.submit(task);
	}

	private Map<String, Object> getIndexMapFromSourceMap(
			Map<String, Object> map, String sourceDomainColumn,
			String sourceKeyColumn, String indexKeyName, String indexColumn) {
		Object domain = map.get(sourceDomainColumn);
		String uuid = null;
		if (domain == null || domain == "") {
			return null;
		}
		uuid = (String) map.get(sourceKeyColumn);
		String domainStr = domain.toString().toLowerCase();
		Map<String, Object> indexMap = new HashMap<>();
		indexMap.put(indexKeyName, domainStr);
		indexMap.put(indexColumn, uuid);
		return indexMap;
	}

	private class IndexTask<T> implements Callable<Boolean> {

		private String table;
		private Map<String, Object> map;

		public IndexTask(String table, Map<String, Object> map) {
			super();
			this.table = table;
			this.map = map;
		}

		@Override
		public Boolean call() throws Exception {
			try {
				dao.save(table, map);
				return Boolean.TRUE;

			} catch (Exception ex) {
				ex.printStackTrace();
				return Boolean.FALSE;
			}
		}

	}
}
