package com.latticeengines.cdl.operationflow.service.impl;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.CleanupAllConfiguration;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.cdl.MaintenanceOperationType;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class CleanupAllServiceUnitTestNG {

    @Mock
    private DataCollectionProxy dataCollectionProxy;
    @Mock
    private DataFeedProxy dataFeedProxy;
    @Mock
    private MetadataProxy metadataProxy;
    @InjectMocks
    private CleanupAllService cleanupAllService;

    @BeforeClass(groups = "unit")
    private void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test(groups = "unit")
    private void testCleanupMethod() {
        CleanupAllConfiguration configuration = new CleanupAllConfiguration();
        CleanupOperationType type = CleanupOperationType.ALL;
        configuration.setCleanupOperationType(type);
        configuration.setCustomerSpace(CleanupAllServiceUnitTestNG.class.getSimpleName());
        configuration.setOperationType(MaintenanceOperationType.DELETE);
        configuration.setEntity(BusinessEntity.Rating);

        Mockito.doNothing().when(dataCollectionProxy).resetTable(anyString(), any(TableRoleInCollection.class));

        Mockito.doNothing().when(dataFeedProxy).resetImport(anyString());
        Mockito.doNothing().when(dataFeedProxy).resetImportByEntity(anyString(), anyString());

        Mockito.doNothing().when(metadataProxy).deleteImportTable(anyString(), anyString());
        Mockito.doNothing().when(metadataProxy).deleteTable(anyString(), anyString());

        try {
            cleanupAllService.invoke(configuration);
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException);
        }
    }
}
