package com.latticeengines.eai.service.impl.vdb;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.eai.VdbConnectorConfiguration;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.eai.exposed.service.EaiService;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;


public class VdbEaiServiceImplDeploymentTestNG extends EaiFunctionalTestNGBase {

    public static final String COLLECTION_DATE_FORMAT = "yyyy-MM-dd-HH-mm-ss";

    private static final long MAX_MILLIS_TO_WAIT = 1000L * 60 * 25;

    @Autowired
    private EaiService eaiService;

    private String customer = "DellEB";

    private CustomerSpace customerSpace = CustomerSpace.parse(customer);

    private Tenant tenant;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        tenant = createTenant(customerSpace.toString());
        try {
            tenantService.discardTenant(tenant);
        } catch (Exception e) {
        }
        tenantService.registerTenant(tenant);
    }

    @Test(groups = { "deployment" }, enabled = false)
    public void extractAndImport() throws Exception {
        VdbConnectorConfiguration vdbConnectorConfiguration = new VdbConnectorConfiguration();
        vdbConnectorConfiguration.setDlDataReady(false);
        vdbConnectorConfiguration.setDlEndpoint("http://le-700074:8888/");
        vdbConnectorConfiguration.setDlTenantId(customer);
        vdbConnectorConfiguration.setDlLoadGroup("G_TargetQuery");

        ImportConfiguration importConfig = new ImportConfiguration();
        importConfig.setCustomerSpace(customerSpace);
        importConfig.setProperty(ImportProperty.IMPORT_CONFIG_STR, JsonUtils.serialize(vdbConnectorConfiguration));

        SourceImportConfiguration sourceImportConfig = new SourceImportConfiguration();
        sourceImportConfig.setSourceType(SourceType.VISIDB);
        importConfig.addSourceConfiguration(sourceImportConfig);
        long startMillis = System.currentTimeMillis();
        ApplicationId appId = eaiService.extractAndImportToHdfs(importConfig);
        assertNotNull(appId);
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
        checkExtractFolderExist(startMillis);
    }

    @AfterClass(groups = "deployment")
    private void cleanUp() throws Exception {
        tenantService.discardTenant(tenant);
    }

    private void checkExtractFolderExist(long startMillis) throws Exception {
        String targetPath = String.format("%s/%s/DataFeed1/DataFeed1-Account/Extracts",
                PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString(),
                SourceType.VISIDB.getName());
        assertTrue(HdfsUtils.fileExists(yarnConfiguration, targetPath));
        List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, targetPath);
        for (String file : files) {
            String filename = file.substring(file.lastIndexOf("/") + 1);
            Date folderTime = new SimpleDateFormat(COLLECTION_DATE_FORMAT).parse(filename);
            long timeDiff = folderTime.getTime() - startMillis;
            if (timeDiff > 0 && timeDiff < MAX_MILLIS_TO_WAIT) {
                return;
            }
            log.info("File name: " + filename);
        }
        assertTrue(false, "No data collection folder was created!");
    }

    protected Tenant createTenant(String customerSpace) {
        Tenant tenant = new Tenant();
        tenant.setId(customerSpace);
        tenant.setName(customerSpace);
        tenant.setRegisteredTime(System.currentTimeMillis());
        return tenant;
    }
}
