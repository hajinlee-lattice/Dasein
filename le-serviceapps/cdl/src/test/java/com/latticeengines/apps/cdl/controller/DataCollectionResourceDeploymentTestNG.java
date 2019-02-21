package com.latticeengines.apps.cdl.controller;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLDataSpace;
import com.latticeengines.domain.exposed.cdl.CDLDataSpace.TableSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection.Version;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;


public class DataCollectionResourceDeploymentTestNG extends CDLDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(DataCollectionResourceDeploymentTestNG.class);

    @Inject
    private CDLTestDataService cdlTestDataService;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        super.setupTestEnvironment();
        mainTestTenant = testBed.getMainTestTenant();
        MultiTenantContext.setTenant(mainTestTenant);
        cdlTestDataService.populateData(mainTestTenant.getId(), 3);
    }

    @Test(groups = "deployment")
    public void testGetCDLDataSpace() {
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        CDLDataSpace cdlDataSpace = dataCollectionProxy.getCDLDataSpace(customerSpace);
        Assert.assertNotNull(cdlDataSpace);
        Assert.assertEquals(cdlDataSpace.getEntities().size(), 5);
        Assert.assertEquals(cdlDataSpace.getActiveVersion(), Version.Blue);
        for (Map.Entry<BusinessEntity, Map<BusinessEntity.DataStore, List<TableSpace>>> entitiesEntry : cdlDataSpace.getEntities().entrySet()) {
            Assert.assertNotNull(entitiesEntry.getValue());
            Assert.assertTrue(entitiesEntry.getValue().size() <= 2);
            for (Map.Entry<BusinessEntity.DataStore, List<TableSpace>> entry : entitiesEntry.getValue().entrySet()) {
                Assert.assertNotNull(entry.getValue());
                List<TableSpace> testTableSpaceList = entry.getValue();
                switch (entry.getKey()) {
                    case Serving:
                        for (TableSpace testTableSpace : testTableSpaceList) {
                            Assert.assertNotNull(testTableSpace.getTables());
                            Assert.assertTrue(CollectionUtils.isNotEmpty(testTableSpace.getTables()));
                        }
                        break;
                    case Batch:
                        for (TableSpace testTableSpace : testTableSpaceList) {
                            Assert.assertNotNull(testTableSpace.getHdfsPath());
                            Assert.assertTrue(CollectionUtils.isNotEmpty(testTableSpace.getHdfsPath()));
                        }
                        break;
                    default:
                        break;
                }
            }
        }
        Assert.assertEquals(cdlDataSpace.getEntities().get(BusinessEntity.PeriodTransaction).get(BusinessEntity.DataStore.Serving).get(0).getTableRole(), TableRoleInCollection.AggregatedPeriodTransaction);
        Assert.assertNotNull(cdlDataSpace.getOthers());

        try {
            log.info(" ");
            //log.info(JsonUtils.serialize(CDLDataSpace));
            log.info(JsonUtils.serialize(cdlDataSpace));
            log.info(" ");

        }
        catch(Exception e) {
            log.info(e.getMessage());
        }
    }

}
