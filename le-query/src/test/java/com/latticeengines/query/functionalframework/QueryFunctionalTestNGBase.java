package com.latticeengines.query.functionalframework;

import static com.latticeengines.query.functionalframework.QueryTestUtils.ATTR_REPO_S3_DIR;
import static com.latticeengines.query.functionalframework.QueryTestUtils.ATTR_REPO_S3_FILENAME;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;

import javax.inject.Inject;

import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
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

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-query-context.xml" })
public class QueryFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

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


    protected static AttributeRepository attrRepo;
    protected static String accountTableName;
    protected static String contactTableName;
    protected static String transactionTableName;
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

    @BeforeClass(groups = "functional")
    public void setupBase() {
        attrRepo = getCustomerAttributeRepo();
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

    protected AttributeRepository getCustomerAttributeRepo(int version) {
        InputStream is = testArtifactService.readTestArtifactAsStream(ATTR_REPO_S3_DIR,
                String.valueOf(version), ATTR_REPO_S3_FILENAME);
        AttributeRepository attrRepo = QueryTestUtils.getCustomerAttributeRepo(is);
        if (version >= 3) {
            for (TableRoleInCollection role: getEntitiesInAttrRepo()) {
                attrRepo.changeServingStoreTableName(role, getServingStoreName(role, version));
            }
        }
        return attrRepo;
    }

    private Collection<TableRoleInCollection> getEntitiesInAttrRepo() {
        return Arrays.asList( //
                TableRoleInCollection.BucketedAccount,
                TableRoleInCollection.SortedContact,
                TableRoleInCollection.AggregatedTransaction,
                TableRoleInCollection.AggregatedPeriodTransaction,
                TableRoleInCollection.CalculatedDepivotedPurchaseHistory,
                TableRoleInCollection.PivotedRating,
                TableRoleInCollection.CalculatedCuratedAccountAttribute,
                TableRoleInCollection.SortedProduct
        );
    }

    private String getServingStoreName(TableRoleInCollection role, int version) {
        return String.format("Query_Test_%s_%d", role, version);
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
