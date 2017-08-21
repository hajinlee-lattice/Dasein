package com.latticeengines.objectapi.functionalframework;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.query.exposed.evaluator.QueryEvaluator;
import com.latticeengines.query.functionalframework.QueryTestUtils;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-objectapi-context.xml" })
public class ObjectApiFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    @Autowired
    protected QueryEvaluator queryEvaluator;

    protected AttributeRepository attrRepo;

    @BeforeClass(groups = "functional")
    public void setupBase() {
        attrRepo = QueryTestUtils.getCustomerAttributeRepo();
    }

}
