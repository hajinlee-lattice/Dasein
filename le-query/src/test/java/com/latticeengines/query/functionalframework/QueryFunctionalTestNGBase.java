package com.latticeengines.query.functionalframework;

import static com.latticeengines.query.functionalframework.QueryTestUtils.ATTR_REPO_S3_DIR;
import static com.latticeengines.query.functionalframework.QueryTestUtils.ATTR_REPO_S3_FILENAME;
import static com.latticeengines.query.functionalframework.QueryTestUtils.TABLEJSONS_S3_FILENAME;
import static com.latticeengines.query.functionalframework.QueryTestUtils.TABLES_S3_FILENAME;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;

import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.SubQuery;
import com.latticeengines.query.evaluator.QueryProcessor;
import com.latticeengines.query.exposed.evaluator.QueryEvaluator;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;
import com.latticeengines.query.exposed.factory.QueryFactory;
import com.latticeengines.query.factory.RedshiftQueryProvider;
import com.latticeengines.testframework.exposed.service.TestArtifactService;
import com.querydsl.sql.SQLQuery;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-query-context.xml" })
public class QueryFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    private static Logger log = LoggerFactory.getLogger(QueryFunctionalTestNGBase.class);

    @Inject
    protected QueryEvaluator queryEvaluator;

    @Inject
    protected QueryEvaluatorService queryEvaluatorService;

    @Inject
    private TestArtifactService testArtifactService;

    @Inject
    protected QueryProcessor queryProcessor;

    @Inject
    protected QueryFactory queryFactory;

    @Inject
    protected Configuration yarnConfiguration;

    @Value("${camille.zk.pod.id}")
    private String podId;

    protected static final String BUCKETED_NOMINAL_ATTR = "TechIndicator_AdobeCreativeSuite";
    protected static final String BUCKETED_PHYSICAL_ATTR = "EAttr354";
    protected static final long BUCKETED_YES_IN_CUSTOEMR = 236;
    protected static final long BUCKETED_NO_IN_CUSTOEMR = 402;
    protected static final long TOTAL_RECORDS = 3170;
    protected static final long BUCKETED_NULL_IN_CUSTOEMR = TOTAL_RECORDS - BUCKETED_YES_IN_CUSTOEMR
            - BUCKETED_NO_IN_CUSTOEMR;

    protected static final String ATTR_ACCOUNT_NAME = "LDC_Name";
    protected static final String ATTR_ACCOUNT_WEBSITE = InterfaceName.Website.name();
    protected static final String ATTR_ACCOUNT_CITY = "LDC_City";

    protected static final String ATTR_CONTACT_TITLE = InterfaceName.Title.name();
    protected static final String ATTR_CONTACT_COUNTRY = InterfaceName.Country.name();

    protected static final String ATTR_ACCOUNT_ID = InterfaceName.AccountId.name();
    protected static final String ATTR_CONTACT_ID = InterfaceName.ContactId.name();
    protected static final String ATTR_CONTACT_EMAIL = InterfaceName.Email.name();
    protected static final String ATTR_TOTAL_AMOUNT = InterfaceName.TotalAmount.name();

    protected static final String ATTR_TRANSACTION_DATE = InterfaceName.TransactionDate.name();
    protected static final String ATTR_PRODUCT_ID = InterfaceName.ProductId.name();

    protected static final String SQL_USER = RedshiftQueryProvider.USER_SEGMENT;

    protected AttributeRepository attrRepo;
    protected CustomerSpace customerSpace;
    protected String accountTableName;
    protected String contactTableName;
    protected String transactionTableName;
    protected Map<String, String> tblPathMap;

    @BeforeClass(groups = "functional")
    public void setupBase() {
        attrRepo = getCustomerAttributeRepo();
    }

    protected long testGetCountAndAssert(String sqlUser, Query query, long expectedCount) {
        long count = queryEvaluatorService.getCount(attrRepo, query, sqlUser);
        Assert.assertEquals(count, expectedCount);
        return count;
    }

    protected List<Map<String, Object>> testGetDataAndAssert(String sqlUser, Query query, long expectedCount,
            List<Map<String, Object>> expectedResult) {
        List<Map<String, Object>> results = queryEvaluatorService.getData(attrRepo, query, sqlUser).getData();
        if (expectedCount >= 0) {
            Assert.assertEquals(results.size(), expectedCount);
        }
        if (expectedResult != null) {
            Assert.assertEquals(results, expectedResult, "Data doesn't match");
        }
        return results;
    }

    protected void logQuery(String sqlUser, SQLQuery<?> sqlQuery) {
        sqlQuery.setUseLiterals(true);
        log.info("sqlUser= {}, sqlQuery = {}", sqlUser, sqlQuery);
    }

    protected Query generateAccountWithSelectedContactQuery(String subSelectAlias) {
        AttributeLookup accountIdAttrLookup = new AttributeLookup(BusinessEntity.Account, ATTR_ACCOUNT_ID);
        Restriction contactRestriction = Restriction.builder().let(BusinessEntity.Contact, ATTR_CONTACT_EMAIL)
                .eq("paul.hopkins@accellent.com").build();
        Restriction accountIdRestriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_ID).eq("0012400001DNVOPAA5")
                .build();

        Query innerQuery = Query.builder().from(BusinessEntity.Contact).where(contactRestriction)
                .select(BusinessEntity.Contact, ATTR_ACCOUNT_ID).build();
        SubQuery subQuery = new SubQuery(innerQuery, subSelectAlias);
        Restriction subQueryRestriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_ID)
                .inCollection(subQuery, ATTR_ACCOUNT_ID).build();

        Restriction accountWithSelectedContact = Restriction.builder().and(accountIdRestriction, subQueryRestriction)
                .build();
        return Query.builder().where(accountWithSelectedContact).select(accountIdAttrLookup)
                .from(BusinessEntity.Account) //
                .build();
    }

    private AttributeRepository getCustomerAttributeRepo() {
        if (attrRepo == null) {
            synchronized (this) {
                if (attrRepo == null) {
                    attrRepo = getCustomerAttributeRepo(1);
                }
            }
        }
        return attrRepo;
    }

    private AttributeRepository getCustomerAttributeRepo(int version) {
        InputStream is = testArtifactService.readTestArtifactAsStream(ATTR_REPO_S3_DIR,
                String.valueOf(version), ATTR_REPO_S3_FILENAME);
        AttributeRepository attrRepo = QueryTestUtils.getCustomerAttributeRepo(is);
        if (version >= 3) {
            for (TableRoleInCollection role: QueryTestUtils.getRolesInAttrRepo()) {
                attrRepo.changeServingStoreTableName(role, QueryTestUtils.getServingStoreName(role, version));
            }
        }
        accountTableName = attrRepo.getTableName(TableRoleInCollection.BucketedAccount);
        contactTableName = attrRepo.getTableName(TableRoleInCollection.SortedContact);
        transactionTableName = attrRepo.getTableName(TableRoleInCollection.AggregatedTransaction);
        return attrRepo;
    }

    protected void initializeAttributeRepo(int version) {
        InputStream is = testArtifactService.readTestArtifactAsStream(ATTR_REPO_S3_DIR,
                String.valueOf(version), ATTR_REPO_S3_FILENAME);
        attrRepo = QueryTestUtils.getCustomerAttributeRepo(is);
        Map<TableRoleInCollection, String> pathMap = readTablePaths(version);
        if (version >= 3) {
            tblPathMap = new HashMap<>();
            for (TableRoleInCollection role: QueryTestUtils.getRolesInAttrRepo()) {
                String tblName = QueryTestUtils.getServingStoreName(role, version);
                String path = pathMap.get(role);
                tblPathMap.put(tblName, path);
                attrRepo.changeServingStoreTableName(role, tblName);
            }
        }
        uploadTablesToHdfs(attrRepo.getCustomerSpace(), version);
        customerSpace = attrRepo.getCustomerSpace();
    }

    private Map<TableRoleInCollection, String> readTablePaths(int version) {
        String downloadsDir = "downloads";
        File downloadedFile = testArtifactService.downloadTestArtifact(ATTR_REPO_S3_DIR, //
                String.valueOf(version), TABLEJSONS_S3_FILENAME);
        String zipFilePath = downloadedFile.getPath();
        try {
            ZipFile zipFile = new ZipFile(zipFilePath);
            zipFile.extractAll(downloadsDir);
        } catch (ZipException e) {
            throw new RuntimeException("Failed to unzip tables archive " + zipFilePath, e);
        }
        Map<TableRoleInCollection, String> pathMap = new HashMap<>();
        QueryTestUtils.getRolesInAttrRepo().forEach(role -> {
            try {
                File tableJsonFile = new File(downloadsDir + File.separator + "TableJsons/" + role + ".json");
                Table table = JsonUtils.deserialize(FileUtils.openInputStream(tableJsonFile), Table.class);
                String path = table.getExtracts().get(0).getPath();
                path = path.replace("/Pods/QA/", "/Pods/" + podId + "/");
                pathMap.put(role, path);
            } catch (IOException e) {
                throw new RuntimeException("Cannot open table json file for " + role);
            }
        });
        return pathMap;
    }

    private void uploadTablesToHdfs(CustomerSpace customerSpace, int version) {
        log.info("Uploading test avros to hdfs ...");
        String downloadsDir = "downloads";
        File downloadedFile = testArtifactService.downloadTestArtifact(ATTR_REPO_S3_DIR, //
                String.valueOf(version), TABLES_S3_FILENAME);
        String zipFilePath = downloadedFile.getPath();
        try {
            ZipFile zipFile = new ZipFile(zipFilePath);
            zipFile.extractAll(downloadsDir);
        } catch (ZipException e) {
            throw new RuntimeException("Failed to unzip tables archive " + zipFilePath, e);
        }
        String targetPath = PathBuilder.buildCustomerSpacePath(podId, customerSpace).append("Data").toString();
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, targetPath)) {
                HdfsUtils.rmdir(yarnConfiguration, targetPath);
            }
            HdfsUtils.copyFromLocalDirToHdfs(yarnConfiguration, downloadsDir, targetPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to copy local dir to hdfs.", e);
        }
    }

    protected void sqlContains(SQLQuery<?> query, String content) {
        Assert.assertTrue(query.toString().toLowerCase().contains(content.toLowerCase()), //
                String.format("Cannot find pattern [%s] in query: %s", content, query));
    }

    protected void sqlNotContain(SQLQuery<?> query, String content) {
        Assert.assertFalse(query.toString().toLowerCase().contains(content.toLowerCase()), //
                String.format("Should not find pattern [%s] in query: %s", content, query));
    }

}
