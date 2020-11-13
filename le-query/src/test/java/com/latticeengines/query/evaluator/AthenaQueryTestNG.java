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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.prestodb.exposed.service.AthenaService;
import com.latticeengines.query.factory.AthenaQueryProvider;
import com.latticeengines.query.functionalframework.QueryFunctionalTestNGBase;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;
import com.querydsl.sql.SQLQuery;

public class AthenaQueryTestNG extends QueryFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(AthenaQueryTestNG.class);

    private final int lowestBit = 50;
    private final int numBits = 4;

    private String tenantId;
    private String accountTableName;
    private String contactTableName;
    private AttributeRepository attrRepo;
    private String accountAvroDir;
    private String contactAvroDir;
    private String accountS3Dir;
    private String contactS3Dir;

    @Value("${common.le.stack}")
    private String leStack;

    @Value("${aws.test.s3.bucket}")
    private String s3Bucket;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Inject
    private AthenaService athenaService;

    @BeforeClass(groups = "functional")
    public void setup() throws IOException {
        tenantId = TestFrameworkUtils.generateTenantName();
        accountTableName = NamingUtils.timestamp(tenantId + "_Account");
        contactTableName = NamingUtils.timestamp(tenantId + "_Contact");

        accountAvroDir = "/tmp/athenaTest/" + leStack + "/account";
        if (HdfsUtils.fileExists(yarnConfiguration, accountAvroDir)) {
            HdfsUtils.rmdir(yarnConfiguration, accountAvroDir);
        }
        contactAvroDir = "/tmp/athenaTest/" + leStack + "/contact";
        if (HdfsUtils.fileExists(yarnConfiguration, contactAvroDir)) {
            HdfsUtils.rmdir(yarnConfiguration, contactAvroDir);
        }
        uploadDataToHdfs(accountAvroDir, contactAvroDir);

        String s3Protocol = Boolean.TRUE.equals(useEmr) ? "s3a" : "s3n";
        String accountS3Prefix = leStack + "/athenaTest/account";
        String contactS3Prefix = leStack + "/athenaTest/contact";
        accountS3Dir = s3Protocol + "://" + s3Bucket + "/" + accountS3Prefix;
        contactS3Dir = s3Protocol + "://" + s3Bucket + "/" + contactS3Prefix;
        copyToS3(accountAvroDir, accountS3Dir);
        copyToS3(contactAvroDir, contactS3Dir);

        athenaService.deleteTableIfExists(accountTableName);
        athenaService.deleteTableIfExists(contactTableName);

        athenaService.createTableIfNotExists(accountTableName, s3Bucket, accountS3Prefix, DataUnit.DataFormat.AVRO);
        athenaService.createTableIfNotExists(contactTableName, s3Bucket, contactS3Prefix, DataUnit.DataFormat.AVRO);

        attrRepo = getAttrRepo();
    }

    @AfterClass(groups = "functional", alwaysRun = true)
    public void tearDown() throws IOException {
        athenaService.deleteTableIfExists(accountTableName);
        athenaService.deleteTableIfExists(contactTableName);
        List<String> dirs = Arrays.asList(accountS3Dir, contactS3Dir, accountAvroDir, contactAvroDir);
        for (String dir: dirs) {
            if (HdfsUtils.fileExists(yarnConfiguration, dir)) {
                HdfsUtils.rmdir(yarnConfiguration, dir);
            }
        }
    }

    @Test(groups = "functional")
    public void testQueryAthena() {
        String sqlUser = AthenaQueryProvider.ATHENA_USER;
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

    private void copyToS3(String hdfsDir, String s3Dir) {
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, s3Dir)) {
                HdfsUtils.rmdir(yarnConfiguration, s3Dir);
            }
            log.info("Copying from {} to {}", hdfsDir, s3Dir);
            HdfsUtils.copyFiles(yarnConfiguration, hdfsDir, s3Dir);
        } catch (IOException e) {
            Assert.fail("Failed to copy from " + hdfsDir + " to " + s3Dir, e);
        }
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
