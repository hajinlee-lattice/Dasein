package com.latticeengines.objectapi.functionalframework;

import static com.latticeengines.query.functionalframework.QueryTestUtils.ATTR_REPO_S3_DIR;
import static com.latticeengines.query.functionalframework.QueryTestUtils.ATTR_REPO_S3_FILENAME;
import static com.latticeengines.query.functionalframework.QueryTestUtils.ATTR_REPO_S3_VERSION;

import java.io.InputStream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.query.exposed.evaluator.QueryEvaluator;
import com.latticeengines.query.functionalframework.QueryTestUtils;
import com.latticeengines.testframework.exposed.service.TestArtifactService;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-objectapi-context.xml" })
public class ObjectApiFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    @Autowired
    protected QueryEvaluator queryEvaluator;

    @Autowired
    private TestArtifactService testArtifactService;

    protected AttributeRepository attrRepo;

    @BeforeClass(groups = "functional")
    public void setupBase() {
        if (attrRepo == null) {
            synchronized (this) {
                if (attrRepo == null) {
                    InputStream is = testArtifactService.readTestArtifactAsStream(ATTR_REPO_S3_DIR,
                            ATTR_REPO_S3_VERSION, ATTR_REPO_S3_FILENAME);
                    attrRepo = QueryTestUtils.getCustomerAttributeRepo(is);
                }
            }
        }
    }

}
