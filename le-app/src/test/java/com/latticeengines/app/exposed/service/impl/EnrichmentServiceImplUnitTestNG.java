package com.latticeengines.app.exposed.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import java.util.List;
import java.util.stream.Collectors;

import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.app.exposed.service.EnrichmentService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.TestDataGenerator;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

public class EnrichmentServiceImplUnitTestNG {
    private static final int INTERNAL_ATTR_COUNT = 200;
    private static final int EXTERNAL_ATTR_COUNT = 200;

    // initialize fake column metadata
    private static final List<ColumnMetadata> TEST_COLUMNS = TestDataGenerator
            .generateColumnMetadataList(INTERNAL_ATTR_COUNT, EXTERNAL_ATTR_COUNT);
    private static final List<ColumnMetadata> TEST_EXTERNAL_COLUMNS = TEST_COLUMNS
            .stream().filter(metadata -> !metadata.getCanInternalEnrich()).collect(Collectors.toList());
    private static final TopNTree TEST_TREE = TestDataGenerator.generateTopNTree(TEST_COLUMNS);
    private static final TopNTree TEST_EXTERNAL_TREE = TestDataGenerator.generateTopNTree(TEST_EXTERNAL_COLUMNS);
    private ColumnMetadataProxy testProxy;
    private BatonService batonService;

    private EnrichmentService svc;


    @BeforeClass(groups = "unit")
    private void setUp() {
        testProxy = Mockito.mock(ColumnMetadataProxy.class);
        Mockito.doNothing().when(testProxy).scheduleLoadColumnMetadataCache();
        Mockito.when(testProxy.columnSelection(ColumnSelection.Predefined.Enrichment)).thenReturn(TEST_COLUMNS);
        Mockito.when(testProxy.getTopNTree()).thenReturn(TEST_TREE);
        Mockito.when(testProxy.getTopNTree(true)).thenReturn(TEST_EXTERNAL_TREE);
        batonService = Mockito.mock(BatonService.class);
        Mockito.when(batonService.getMaxDataLicense(any(Category.class), anyString())).thenReturn(1);
        svc = new EnrichmentServiceImpl();
        // replace with the mock proxy
        ReflectionTestUtils.setField(svc, "columnMetadataProxy", testProxy);
        ReflectionTestUtils.setField(svc, "batonService", batonService);
    }

    /**
     * make sure {@link EnrichmentServiceImpl#getTopNTree(boolean)} works
     * under concurrent access
     */
    @Test(groups = "unit", threadPoolSize = 10, invocationCount = 10)
    public void concurrentGetTopNTree() {
        try {
            TopNTree tree = svc.getTopNTree(true);
            Assert.assertNotNull(tree);
        } catch (Exception e) {
            Assert.fail("Should not throw any exception");
        }
    }
}
