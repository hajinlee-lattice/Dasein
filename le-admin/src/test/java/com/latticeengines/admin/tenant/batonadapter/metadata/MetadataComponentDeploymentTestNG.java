package com.latticeengines.admin.tenant.batonadapter.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.latticeengines.admin.service.impl.ComponentOrchestrator;
import com.latticeengines.admin.service.impl.TenantServiceImpl.ProductAndExternalAdminInfo;
import com.latticeengines.admin.tenant.batonadapter.BatonAdapterDeploymentTestNGBase;
import com.latticeengines.admin.tenant.batonadapter.pls.PLSComponent;
import com.latticeengines.admin.tenant.batonadapter.pls.PLSComponentDeploymentTestNG;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.metadata.Table;

public class MetadataComponentDeploymentTestNG extends BatonAdapterDeploymentTestNGBase {

    @Autowired
    private ComponentOrchestrator orchestrator;

    @Autowired
    private PLSComponentDeploymentTestNG plsComponentTestNG;

    @Value("${admin.metadata.url}")
    private String metadataUrl;

    public void installMetadata() {
        Map<String, Map<String, String>> properties = new HashMap<>();
        DocumentDirectory confDir = plsComponentTestNG.getPLSDocumentDirectory();
        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(confDir);
        properties.put(PLSComponent.componentName, sDir.flatten());

        sDir = new SerializableDocumentDirectory(batonService.getDefaultConfiguration(getServiceName()));
        properties.put(getServiceName(), sDir.flatten());
        ProductAndExternalAdminInfo prodAndExternalAminInfo = super.generateLPAandEmptyExternalAdminInfo();
        orchestrator.orchestrate(contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, properties,
                prodAndExternalAminInfo);
    }

    @Test(groups = "deployment")
    public void testInstallation() throws InterruptedException, IOException {
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        installMetadata();
        // verify parent component, for debugging purpose
        BootstrapState state = waitForSuccess(MetadataComponent.componentName);
        Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);
        state = waitForSuccess(getServiceName());
        Assert.assertEquals(state.state, BootstrapState.State.OK);
        Assert.assertEquals(getImportTables(CustomerSpace.parse(contractId).toString()).size(), 5);
    }

    public List<Table> getImportTables(String customerSpace) {
        Map<String, String> uriVariables = new HashMap<>();
        uriVariables.put("customerSpace", customerSpace);
        String[] tableNames = restTemplate.getForObject(metadataUrl + "/customerspaces/{customerSpace}/importtables",
                String[].class, uriVariables);
        List<Table> tables = new ArrayList<>();
        for (String tableName : tableNames) {
            uriVariables.put("tableName", tableName);
            Table table = getImportTable(customerSpace, tableName);
            if (table != null) {
                tables.add(table);
            }
        }
        return tables;
    }

    private Table getImportTable(String customerSpace, String tableName) {
        Map<String, String> uriVariables = new HashMap<>();
        uriVariables.put("customerSpace", customerSpace);
        uriVariables.put("tableName", tableName);
        Table newTable = restTemplate.getForObject(metadataUrl
                + "/customerspaces/{customerSpace}/importtables/{tableName}", Table.class, uriVariables);
        return newTable;
    }

    @Override
    protected String getServiceName() {
        return MetadataComponent.componentName;
    }

    @AfterClass(groups = "deployment")
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        plsComponentTestNG.tearDown(contractId, tenantId);
    }

}
