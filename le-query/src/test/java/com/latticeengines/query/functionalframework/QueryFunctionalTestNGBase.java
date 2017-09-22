package com.latticeengines.query.functionalframework;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.SubQuery;
import com.latticeengines.query.exposed.evaluator.QueryEvaluator;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-query-context.xml" })
public class QueryFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    @Autowired
    protected QueryEvaluator queryEvaluator;

    @Autowired
    protected QueryEvaluatorService queryEvaluatorService;

    protected static AttributeRepository attrRepo;
    protected static String accountTableName;
    protected static String contactTableName;
    protected static final String BUCKETED_NOMINAL_ATTR = "TechIndicator_AdobeCreativeSuite";
    protected static final String BUCKETED_PHYSICAL_ATTR = "EAttr220";
    protected static final long BUCKETED_YES_IN_CUSTOEMR = 39;
    protected static final long BUCKETED_NO_IN_CUSTOEMR = 29;
    protected static final long TOTAL_RECORDS = 506574;
    protected static final long BUCKETED_NULL_IN_CUSTOEMR = TOTAL_RECORDS - BUCKETED_YES_IN_CUSTOEMR
            - BUCKETED_NO_IN_CUSTOEMR;

    protected static final String ATTR_ACCOUNT_NAME = InterfaceName.CompanyName.name();
    protected static final String ATTR_ACCOUNT_WEBSITE = InterfaceName.Website.name();
    protected static final String ATTR_ACCOUNT_CITY = InterfaceName.City.name();

    protected static final String ATTR_CONTACT_TITLE = InterfaceName.Title.name();
    protected static final String ATTR_CONTACT_COUNTRY = InterfaceName.Country.name();

    protected static final String ATTR_ACCOUNT_ID = InterfaceName.AccountId.name();
    protected static final String ATTR_CONTACT_ID = InterfaceName.ContactId.name();
    protected static final String ATTR_CONTACT_EMAIL = InterfaceName.Email.name();

    @BeforeClass(groups = "functional")
    public void setupBase() {
        attrRepo = getCustomerAttributeRepo();
    }

    protected Query generateAccountWithSelectedContactQuery(String subSelectAlias) {

        AttributeLookup accountIdAttrLookup = new AttributeLookup(BusinessEntity.Account, ATTR_ACCOUNT_ID);
        Restriction contactRestriction = Restriction.builder().let(BusinessEntity.Contact, ATTR_CONTACT_EMAIL)
                .eq("avelayudham@worldbank.org.kb").build();
        Restriction accountIdRestriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_ID).eq(1802)
                .build();

        Query innerQuery = Query.builder().from(BusinessEntity.Contact).where(contactRestriction)
                .select(BusinessEntity.Contact, ATTR_ACCOUNT_ID).build();
        SubQuery subQuery = new SubQuery(innerQuery, subSelectAlias);
        Restriction subQueryRestriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_ID)
                .inCollection(subQuery, ATTR_ACCOUNT_ID).build();

        Restriction accountWithSelectedContact = Restriction.builder().and(accountIdRestriction, subQueryRestriction)
                .build();
        Query query = Query.builder().where(accountWithSelectedContact).select(accountIdAttrLookup)
                .from(BusinessEntity.Account) //
                .build();
        return query;
    }

    private static AttributeRepository getCustomerAttributeRepo() {
        if (attrRepo == null) {
            attrRepo = QueryTestUtils.getCustomerAttributeRepo();
            synchronized (QueryFunctionalTestNGBase.class) {
                accountTableName = attrRepo.getTableName(TableRoleInCollection.BucketedAccount);
                contactTableName = attrRepo.getTableName(TableRoleInCollection.SortedContact);
            }
        }
        return attrRepo;
    }

}
