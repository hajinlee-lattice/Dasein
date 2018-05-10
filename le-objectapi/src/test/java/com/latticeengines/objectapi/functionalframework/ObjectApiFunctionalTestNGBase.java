package com.latticeengines.objectapi.functionalframework;

import java.io.InputStream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.query.exposed.evaluator.QueryEvaluator;
import com.latticeengines.query.functionalframework.QueryTestUtils;
import com.latticeengines.testframework.exposed.service.TestArtifactService;

import static com.latticeengines.query.functionalframework.QueryTestUtils.ATTR_REPO_S3_DIR;
import static com.latticeengines.query.functionalframework.QueryTestUtils.ATTR_REPO_S3_FILENAME;
import static com.latticeengines.query.functionalframework.QueryTestUtils.ATTR_REPO_S3_VERSION;

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

    public EventFrontEndQuery loadFrontEndQueryFromResource(String resourceName) {
        try {
            InputStream inputStream = ClassLoader.class.getResourceAsStream(resourceName);
            return JsonUtils.deserialize(inputStream, EventFrontEndQuery.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load json resource" + resourceName, e);
        }

    }

}
