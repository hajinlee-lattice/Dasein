package com.latticeengines.admin.tenant.batonadapter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.admin.configurationschema.ConfigurationSchemaTestNGBase;
import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

import junit.framework.Assert;

/**
 * besides the same setup and teardown as AdminFunctionalTestNGBase,
 * we also register the testing component's installer,
 * in case it has not been registered already by ServiceServiceImpl
 */
public abstract class BatonAdapterBaseDeploymentTestNG<T extends LatticeComponent> extends AdminFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(BatonAdapterBaseDeploymentTestNG.class);
    private String contractId, tenantId, serviceName;

    @BeforeClass(groups = "deployment")
    public void setup() {
        serviceName = getServiceName();
        contractId = serviceName + "-contract";
        tenantId = serviceName + "-tenant";

        loginAD();
        try {
            deleteTenant(contractId, tenantId);
        } catch (Exception e) {
            //ignore
        }
        createTenant(contractId, tenantId);

    }

    @AfterClass(groups = "deployment")
    public void tearDown() throws Exception {
        try {
            deleteTenant(contractId, tenantId);
        } catch (Exception e) {
            //ignore
        }
    }

    @Test(groups = "deployment")
    public void getDefaultConfig() throws Exception {
        testGetDefaultConfig(getExpectedJsonFile());
    }

    protected abstract String getServiceName();

    protected void bootstrap(DocumentDirectory confDir) { super.bootstrap(contractId, tenantId, serviceName, confDir); }

    private void testGetDefaultConfig(String expectedJson) {
        String url = String.format("%s/admin/services/%s/default", getRestHostPort(), serviceName);
        SerializableDocumentDirectory serializableDir =
                restTemplate.getForObject(url, SerializableDocumentDirectory.class);
        Assert.assertNotNull(serializableDir);

        DocumentDirectory dir = SerializableDocumentDirectory.deserialize(serializableDir);
        dir.makePathsLocal();
        serializableDir = new SerializableDocumentDirectory(dir);
        DocumentDirectory metaDir = batonService.getConfigurationSchema(serviceName);
        serializableDir.applyMetadata(metaDir);

        ConfigurationSchemaTestNGBase.assertSerializableDirAndJsonAreEqual(serializableDir, expectedJson);
    }

    protected abstract String getExpectedJsonFile();
}
