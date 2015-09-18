package com.latticeengines.propdata.matching.service.impl;

import java.util.Map;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.latticeengines.db.dao.cassandra.CassandraGenericDao;
import com.latticeengines.db.dao.cassandra.data.ColumnTypeMapper;
import com.latticeengines.propdata.matching.service.MatchService;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:propdata-matching-context.xml", "classpath:propdata-matching-properties-context.xml" })
public class MatchServicemplTestNG extends AbstractTestNGSpringContextTests {

    @Resource(name = "propDataCassandraGenericDao")
    private CassandraGenericDao dao;

    @Autowired
    private MatchService matchService;

    @Test(groups = "manual")
    public void createDomainIndex() {

        matchService.createDomainIndex("HGData_Source");
        matchService.createDomainIndex("BuiltWithDomain");
        matchService.createDomainIndex("Experian_Source");
    }

    @Test(groups = "manual")
    public void lookupIndex() {
        long startTime = System.currentTimeMillis();

        String domainIndexTable = "Domain_Index";
        dao.createIndex(domainIndexTable, "HGData_Source", "HGData_Source_Index");
        dao.createIndex(domainIndexTable, "BuiltWithDomain", "BuiltWithDomain_Index");
        dao.createIndex(domainIndexTable, "Experian_Source", "Experian_Source_Index");

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
                Assert.assertNotNull(dao.findByKey2("\"HGData_Source\"", "uuid", hgDataUUID));
                sourceCount++;
            }

            if (builtWithUUID != null) {
                Assert.assertNotNull(dao.findByKey2("\"BuiltWithDomain\"", "uuid", builtWithUUID));
                sourceCount++;
            }

            if (experianUUID != null) {
                Assert.assertNotNull(dao.findByKey2("\"Experian_Source\"", "uuid", experianUUID));
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
        System.out.println("Index lookup total time=" + (System.currentTimeMillis() - startTime) / 1000 + " seconds.");
    }

}