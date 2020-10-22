package com.latticeengines.query.evaluator;

import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.BitCodecUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.prestodb.exposed.service.PrestoDbService;
import com.latticeengines.query.factory.PrestoQueryProvider;
import com.latticeengines.query.functionalframework.QueryFunctionalTestNGBase;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;
import com.querydsl.sql.SQLQuery;

public class PrestoQueryTestNG extends QueryFunctionalTestNGBase {

    private final int lowestBit = 50;
    private final int numBits = 4;

    private String tenantId;
    private String accountTableName;
    private String contactTableName;
    private AttributeRepository attrRepo;
    private String accountAvroDir;
    private String contactAvroDir;

    @Value("${common.le.stack}")
    private String leStack;

    @Inject
    private PrestoDbService prestoDbService;

    @BeforeClass(groups = "functional")
    public void setup() throws IOException {
        tenantId = TestFrameworkUtils.generateTenantName();
        accountTableName = NamingUtils.timestamp(tenantId + "_Account");
        contactTableName = NamingUtils.timestamp(tenantId + "_Contact");

        accountAvroDir = "/tmp/prestoTest/" + leStack + "/account";
        if (HdfsUtils.fileExists(yarnConfiguration, accountAvroDir)) {
            HdfsUtils.rmdir(yarnConfiguration, accountAvroDir);
        }
        contactAvroDir = "/tmp/prestoTest/" + leStack + "/contact";
        if (HdfsUtils.fileExists(yarnConfiguration, contactAvroDir)) {
            HdfsUtils.rmdir(yarnConfiguration, contactAvroDir);
        }
        uploadDataToHdfs(accountAvroDir, contactAvroDir);

        prestoDbService.deleteTableIfExists(accountTableName);
        prestoDbService.createTableIfNotExists(accountTableName, accountAvroDir);
        prestoDbService.deleteTableIfExists(contactTableName);
        prestoDbService.createTableIfNotExists(contactTableName, contactAvroDir);

        attrRepo = getAttrRepo();
    }

    @AfterClass(groups = "functional", alwaysRun = true)
    public void tearDown() throws IOException {
        prestoDbService.deleteTableIfExists(accountTableName);
        prestoDbService.deleteTableIfExists(contactTableName);
        if (HdfsUtils.fileExists(yarnConfiguration, accountAvroDir)) {
            HdfsUtils.rmdir(yarnConfiguration, accountAvroDir);
        }
        if (HdfsUtils.fileExists(yarnConfiguration, contactAvroDir)) {
            HdfsUtils.rmdir(yarnConfiguration, contactAvroDir);
        }
    }

    @Test(groups = "functional")
    public void testQueryPresto() {
        String sqlUser = PrestoQueryProvider.PRESTO_USER;

        Restriction restriction = Restriction.builder() //
                .let(Account, "AccountId").eq("acc1") //
                .build();
        Query query = Query.builder() //
                .find(Account) //
                .where(restriction) //
                .build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(attrRepo, sqlUser, query, 1);

        restriction = Restriction.builder() //
                .let(Account, "AccountId").eq("acc1") //
                .build();
        query = Query.builder() //
                .find(Contact) //
                .where(restriction) //
                .build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(attrRepo, sqlUser, query, 2);

        restriction = Restriction.builder() //
                .let(Contact, "ContactName").contains("Contact") //
                .build();
        query = Query.builder() //
                .find(Account) //
                .where(restriction) //
                .build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(attrRepo, sqlUser, query, 4);

        restriction = Restriction.builder() //
                .let(Account, "BitEncoded").inCollection( //
                        Arrays.asList("Category 5", "Category 7")) //
                .build();
        query = Query.builder() //
                .find(Account) //
                .where(restriction) //
                .build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(attrRepo, sqlUser, query, 2);

        restriction = Restriction.builder() //
                .let(Account, "AccountId").eq("acc2") //
                .build();
        query = Query.builder() //
                .select(Account, "CompanyName") //
                .from(Account) //
                .where(restriction) //
                .build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        List<Map<String, Object>> expectedResult = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("CompanyName", null);
        expectedResult.add(row);
        testGetDataAndAssert(attrRepo, sqlUser, query, 1, expectedResult);
    }

    private void uploadDataToHdfs(String accountAvroDir, String contactAvroDir) {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("AccountId", String.class),
                Pair.of("CompanyName", String.class),
                Pair.of("EAttr", Long.class)
        );
        Object[][] data = new Object[][]{
                {"acc1", "Company 1", getEncodedValue(5)},
                {"acc2", null, getEncodedValue(7)},
        };
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, accountAvroDir)) {
                HdfsUtils.rmdir(yarnConfiguration, accountAvroDir);
            }
            AvroUtils.uploadAvro(yarnConfiguration, data, fields, "account", accountAvroDir);
        } catch (Exception e) {
            Assert.fail("Failed to upload account data to hdfs", e);
        }

        fields = Arrays.asList( //
                Pair.of("ContactId", String.class),
                Pair.of("AccountId", String.class),
                Pair.of("ContactName", String.class)
        );
        data = new Object[][]{
                {"contact1", "acc1", "Contact 1"},
                {"contact2", "acc1", "Contact 2"},
                {"contact3", "acc2", "Contact 3"},
                {"contact4", "acc2", "Contact 4"}
        };
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, contactAvroDir)) {
                HdfsUtils.rmdir(yarnConfiguration, contactAvroDir);
            }
            AvroUtils.uploadAvro(yarnConfiguration, data, fields, "contact", contactAvroDir);
        } catch (Exception e) {
            Assert.fail("Failed to upload contact data to hdfs", e);
        }
    }

    private long getEncodedValue(int val) {
        long encoded = 0L;
        return BitCodecUtils.setBits(encoded, lowestBit, numBits, val);
    }

    private AttributeRepository getAttrRepo() {
        Map<AttributeLookup, ColumnMetadata> cmMap = new HashMap<>();
        cmMap.put(new AttributeLookup(Account, "AccountId"), //
                new ColumnMetadata("AccountId", "String"));
        cmMap.put(new AttributeLookup(Account, "CompanyName"), //
                new ColumnMetadata("CompanyName", "String"));
        ColumnMetadata bitEncodedCol = new ColumnMetadata("BitEncoded", "String");
        bitEncodedCol.setBitOffset(lowestBit);
        bitEncodedCol.setNumBits(numBits);
        bitEncodedCol.setPhysicalName("EAttr");
        AttributeStats attrStats = new AttributeStats();
        attrStats.setNonNullCount(2L);
        List<Bucket> bucketList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Bucket bucket = new Bucket();
            bucket.setId((long) i);
            bucket.setLabel("Category " + i);
            bucketList.add(bucket);
        }
        Buckets buckets = new Buckets();
        buckets.setBucketList(bucketList);
        attrStats.setBuckets(buckets);
        bitEncodedCol.setStats(attrStats);
        cmMap.put(new AttributeLookup(Account, "BitEncoded"), bitEncodedCol);

        cmMap.put(new AttributeLookup(Contact, "AccountId"), //
                new ColumnMetadata("AccountId", "String"));
        cmMap.put(new AttributeLookup(Contact, "ContactId"), //
                new ColumnMetadata("ContactId", "String"));
        cmMap.put(new AttributeLookup(Contact, "ContactName"), //
                new ColumnMetadata("ContactName", "String"));

        Map<TableRoleInCollection, String> tableNameMap = new HashMap<>();
        tableNameMap.put(TableRoleInCollection.BucketedAccount, accountTableName);
        tableNameMap.put(TableRoleInCollection.SortedContact, contactTableName);

        return new AttributeRepository( //
                CustomerSpace.parse(tenantId), "default", null, cmMap, tableNameMap);
    }

}
