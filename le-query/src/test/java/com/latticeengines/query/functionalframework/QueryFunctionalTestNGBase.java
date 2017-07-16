package com.latticeengines.query.functionalframework;

import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;

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

    protected AttributeRepository attrRepo;

    @BeforeClass(groups = "functional")
    public void setupBase() {
        attrRepo = QueryTestUtils.getCustomerAttributeRepo();
        ColumnMetadataProxy proxy = Mockito.mock(ColumnMetadataProxy.class);
        Mockito.when(proxy.getAttrRepo()).thenReturn(QueryTestUtils.getAMAttributeRepo());
        attrRepoUtils.setColumnMetadataProxy(proxy);
    }

}
