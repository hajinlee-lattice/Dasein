package com.latticeengines.query.functionalframework;

import java.io.InputStream;

import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.query.exposed.evaluator.QueryEvaluator;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;
import com.latticeengines.query.util.AttrRepoUtils;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-query-context.xml" })
public class QueryFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    @Autowired
    protected QueryEvaluator queryEvaluator;

    @Autowired
    protected QueryEvaluatorService queryEvaluatorService;

    @Autowired
    private AttrRepoUtils attrRepoUtils;

    protected static AttributeRepository attrRepo;
    private static AttributeRepository amAttrRepo;
    protected static String accountTableName;
    protected static String amTableName;

    protected static final String BUCKETED_NOMINAL_ATTR = "TechIndicator_Lexity";
    protected static final String BUCKETED_PHYSICAL_ATTR = "EAttr394";
    protected static final long  BUCKETED_YES_IN_CUSTOEMR = 2095;
    protected static final long  BUCKETED_NO_IN_CUSTOEMR = 39251;
    protected static final long  BUCKETED_NULL_IN_CUSTOEMR = 100000 - BUCKETED_YES_IN_CUSTOEMR - BUCKETED_NO_IN_CUSTOEMR;


    @BeforeClass(groups = "functional")
    public void setupBase() {
        attrRepo = getCustomerAttributeRepo();
        ColumnMetadataProxy proxy = Mockito.mock(ColumnMetadataProxy.class);
        Mockito.when(proxy.getAttrRepo()).thenReturn(getAMAttributeRepo());
        attrRepoUtils.setColumnMetadataProxy(proxy);
    }

    private static AttributeRepository getCustomerAttributeRepo() {
        if (attrRepo == null) {
            synchronized (QueryFunctionalTestNGBase.class) {
                InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("attrrepo.json");
                attrRepo = JsonUtils.deserialize(is, AttributeRepository.class);
                accountTableName = attrRepo.getTableName(TableRoleInCollection.BucketedAccount);
            }
        }
        return attrRepo;
    }

    private static AttributeRepository getAMAttributeRepo() {
        if (amAttrRepo == null) {
            synchronized (QueryFunctionalTestNGBase.class) {
                InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("am_attrrepo.json");
                amAttrRepo = JsonUtils.deserialize(is, AttributeRepository.class);
                amTableName = amAttrRepo.getTableName(TableRoleInCollection.AccountMaster);
            }
        }
        return amAttrRepo;
    }

}
