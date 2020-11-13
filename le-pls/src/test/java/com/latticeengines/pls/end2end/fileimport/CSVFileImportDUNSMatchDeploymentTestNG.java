package com.latticeengines.pls.end2end.fileimport;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ATTR_LDC_DUNS;
import static com.latticeengines.domain.exposed.pls.ActionType.CDL_DATAFEED_IMPORT_WORKFLOW;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class CSVFileImportDUNSMatchDeploymentTestNG extends CSVFileImportDeploymentTestNGBase {

    @Inject
    private ActionProxy actionProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @BeforeClass(groups = "deployment.import.group2")
    public void setup() throws Exception {
        String featureFlag = LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName();
        Map<String, Boolean> flags = new HashMap<>();
        flags.put(featureFlag, true);
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG, flags);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        createDefaultImportSystem();
    }

    @Test(groups = "deployment.import.group2")
    public void testImport() throws Exception {
        prepareBaseData(ENTITY_ACCOUNT);
        getDataFeedTask(ENTITY_ACCOUNT);

        Table accountTemplate = accountDataFeedTask.getImportTemplate();
        Assert.assertNull(accountTemplate.getAttribute(InterfaceName.AccountId));
        Assert.assertNull(accountTemplate.getAttribute(ATTR_LDC_DUNS));

        List<Action> actionList = actionProxy.getActions(customerSpace);
        Assert.assertTrue(CollectionUtils.isNotEmpty(actionList));
        actionList = actionList.stream()
                .filter(action -> action.getType().equals(CDL_DATAFEED_IMPORT_WORKFLOW))
                .collect(Collectors.toList());
        Assert.assertEquals(CollectionUtils.size(actionList), 1);
        ImportActionConfiguration importConfig = (ImportActionConfiguration)actionList.get(0).getActionConfiguration();
        Assert.assertEquals(CollectionUtils.size(importConfig.getRegisteredTables()), 1);
        Table importTable = metadataProxy.getTable(customerSpace, importConfig.getRegisteredTables().get(0));
        Assert.assertNotNull(importTable);
        Assert.assertNotNull(importTable.getAttribute(ATTR_LDC_DUNS));
        Assert.assertTrue(CollectionUtils.isNotEmpty(importTable.getExtracts()));
        List<String> dataFiles = HdfsUtils.getFilesByGlob(yarnConfiguration,
                importTable.getExtracts().get(0).getPath());
        Assert.assertTrue(CollectionUtils.isNotEmpty(dataFiles));
        Schema schema = AvroUtils.getSchema(yarnConfiguration, new Path(dataFiles.get(0)));

        Assert.assertNotNull(schema.getField(ATTR_LDC_DUNS));
    }
}
