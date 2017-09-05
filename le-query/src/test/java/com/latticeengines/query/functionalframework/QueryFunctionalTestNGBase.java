package com.latticeengines.query.functionalframework;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
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

    protected static final String BUCKETED_NOMINAL_ATTR = "TechIndicator_AdobeCreativeSuite";
    protected static final String BUCKETED_PHYSICAL_ATTR = "EAttr220";
    protected static final long  BUCKETED_YES_IN_CUSTOEMR = 39;
    protected static final long  BUCKETED_NO_IN_CUSTOEMR = 29;
    protected static final long  BUCKETED_NULL_IN_CUSTOEMR = 506574 - BUCKETED_YES_IN_CUSTOEMR - BUCKETED_NO_IN_CUSTOEMR;

    protected static final String ATTR_ACCOUNT_NAME = InterfaceName.CompanyName.name();
    protected static final String ATTR_ACCOUNT_WEBSITE = InterfaceName.Website.name();
    protected static final String ATTR_ACCOUNT_CITY = InterfaceName.City.name();

    protected static final String ATTR_CONTACT_TITLE = InterfaceName.Title.name();
    protected static final String ATTR_CONTACT_COUNTRY = InterfaceName.Country.name();

    protected static final String ATTR_ACCOUNT_ID = InterfaceName.AccountId.name();

    @BeforeClass(groups = "functional")
    public void setupBase() {
        attrRepo = getCustomerAttributeRepo();
    }

    private static AttributeRepository getCustomerAttributeRepo() {
        if (attrRepo == null) {
            attrRepo = QueryTestUtils.getCustomerAttributeRepo();
            synchronized (QueryFunctionalTestNGBase.class) {
                accountTableName = attrRepo.getTableName(TableRoleInCollection.BucketedAccount);
            }
        }
        return attrRepo;
    }

}
