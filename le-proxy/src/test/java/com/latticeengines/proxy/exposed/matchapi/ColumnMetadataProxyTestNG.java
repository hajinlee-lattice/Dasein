package com.latticeengines.proxy.exposed.matchapi;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.List;

import javax.inject.Inject;

import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.TestDataGenerator;
import com.latticeengines.domain.exposed.metadata.statistics.TopAttribute;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

@ContextConfiguration(locations = "classpath:test-proxy-context.xml")
public class ColumnMetadataProxyTestNG extends AbstractTestNGSpringContextTests {
    private static final String TEST_SUB_CATEGORY = TestDataGenerator.TEST_SUB_CATEGORY;
    private static final int INTERNAL_ATTR_COUNT = 200;
    private static final int EXTERNAL_ATTR_COUNT = 200;
    // initialize fake column metadata
    private static final List<ColumnMetadata> TEST_COLUMNS = TestDataGenerator
            .generateColumnMetadataList(INTERNAL_ATTR_COUNT, EXTERNAL_ATTR_COUNT);

    private TestColumnMetadataProxy testProxy;

    @Inject
    private TestColumnMetadataProxyTag testProxyTag;

    @BeforeClass(groups = "functional")
    public void setUp() {
        // casting from tag to test proxy
        testProxy = (TestColumnMetadataProxy) testProxyTag;
        testProxy = Mockito.spy(testProxy);
        Mockito.when(testProxy.get(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenAnswer((invocation) -> TestDataGenerator.generateTopNTree(TEST_COLUMNS));
        Mockito.when(testProxy.getKryo(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenAnswer((invocation) -> TestDataGenerator.generateTopNTree(TEST_COLUMNS));
        Mockito.when(testProxy.columnSelection(ColumnSelection.Predefined.Enrichment)).thenReturn(TEST_COLUMNS);
    }

    @Test(groups = "functional")
    public void getTopNTreeExcludeInternalEnrichment() {
        TopNTree entireTree = testProxy.getTopNTree();
        TopNTree entireTree2 = testProxy.getTopNTree(false);
        TopNTree internalExcludedTree = testProxy.getTopNTree(true);
        assertNotNull(entireTree);
        assertNotNull(entireTree2);
        assertNotNull(internalExcludedTree);

        // check if the proxy use the same object for the entire tree & internal excluded one
        assertNotEquals(entireTree, internalExcludedTree);
        assertNotEquals(entireTree2, internalExcludedTree);

        int total = INTERNAL_ATTR_COUNT + EXTERNAL_ATTR_COUNT;
        // check size
        List<TopAttribute> entireAttributes = entireTree
                .getCategory(Category.DEFAULT).getSubcategory(TEST_SUB_CATEGORY);
        List<TopAttribute> entireAttributes2 = entireTree2
                .getCategory(Category.DEFAULT).getSubcategory(TEST_SUB_CATEGORY);
        List<TopAttribute> internalAttributes = internalExcludedTree
                .getCategory(Category.DEFAULT).getSubcategory(TEST_SUB_CATEGORY);
        assertEquals(entireAttributes.size(), total);
        assertEquals(entireAttributes2.size(), total);
        assertEquals(internalAttributes.size(),
                EXTERNAL_ATTR_COUNT);

        // check attribute type for internal excluded tree
        for (TopAttribute attr : internalAttributes) {
            assertTrue(attr.getAttribute().startsWith(TestDataGenerator.EXTERNAL_ATTR_PREFIX));
        }
    }

    /**
     * make sure {@link ColumnMetadataProxy#getTopNTree(boolean)} works
     * under concurrent access
     */
    @Test(groups = "functional", threadPoolSize = 20, invocationCount = 20)
    public void concurrentGetTopNTreeExcludeInternalEnrichment() {
        TopNTree tree = testProxy.getTopNTree(true);
        assertNotNull(tree);

        // make sure attributes that can be internal enriched are correctly filtered out
        List<TopAttribute> attributes = tree.getCategory(Category.DEFAULT).getSubcategory(TEST_SUB_CATEGORY);
        assertNotNull(attributes);
        assertEquals(attributes.size(), EXTERNAL_ATTR_COUNT);
        for (TopAttribute attr : attributes) {
            assertNotNull(attr);
            assertTrue(attr.getAttribute().startsWith(TestDataGenerator.EXTERNAL_ATTR_PREFIX));
        }
    }

    /**
     * Test configuration for injecting {@link TestColumnMetadataProxy}
     */
    @Configuration
    static class TestConfig {
        @Bean()
        public TestColumnMetadataProxyTag getProxy() {
            return new TestColumnMetadataProxy();
        }
    }

    /**
     * Extend {@link ColumnMetadataProxy} and override required methods for package access and mocking in
     * the tests.
     */
    static class TestColumnMetadataProxy extends ColumnMetadataProxy implements TestColumnMetadataProxyTag {
        @Override
        protected <T> T getKryo(String method, String url, Class<T> returnValueClazz) {
            return null;
        }

        @Override
        protected <T> T get(String method, String url, Class<T> returnValueClazz) {
            return null;
        }

        @Override
        public List<ColumnMetadata> columnSelection(ColumnSelection.Predefined selectName) {
            return null;
        }
    }

    /**
     * Tag interface for injecting {@link TestColumnMetadataProxy} without affecting the original
     * {@link ColumnMetadataProxy}
     */
    interface TestColumnMetadataProxyTag {}
}

