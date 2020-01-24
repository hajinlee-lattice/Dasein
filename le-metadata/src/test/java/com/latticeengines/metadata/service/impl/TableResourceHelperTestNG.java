package com.latticeengines.metadata.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;

import javax.inject.Inject;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.MetadataService;
public class TableResourceHelperTestNG extends MetadataFunctionalTestNGBase {

    @Inject
    private MetadataProvisioningServiceImplTestNG metadataProvisioningServiceImplTestNG;

    @Inject
    private TableResourceHelper tableResourceHelper;

    private CustomerSpace space;

    @Inject
    private MetadataService mdService;

    @BeforeClass(groups = "functional")
    public void setup() {
        metadataProvisioningServiceImplTestNG.setup();
        space = metadataProvisioningServiceImplTestNG.getCustomerSpace();
    }

    @Test(groups = { "functional" })
    public void resetTables() throws Exception {
        metadataProvisioningServiceImplTestNG.provisionImportTables();
        List<Table> tables = mdService.getImportTables(space);
        assertEquals(tables.size(), 5);
        assertTrue(tableResourceHelper.resetTables(space.toString()));
        assertEquals(tables.size(), 5);
        List<Table> newTables = mdService.getImportTables(space);
        for (int i = 0; i < tables.size(); i++) {
            assertTrue(tables.get(i).getLastModifiedKey().getLastModifiedTimestamp() < newTables.get(i)
                    .getLastModifiedKey().getLastModifiedTimestamp());
        }
    }

    @AfterClass(groups = "functional")
    private void cleanUp() {
        metadataProvisioningServiceImplTestNG.cleanUp();
    }
}
