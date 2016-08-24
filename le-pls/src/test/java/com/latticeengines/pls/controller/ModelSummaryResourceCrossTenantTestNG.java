package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;

import java.net.URL;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class ModelSummaryResourceCrossTenantTestNG extends PlsFunctionalTestNGBase {

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Autowired
    private Configuration yarnConfiguration;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupMarketoEloquaTestEnvironment();

        String dir = modelingServiceHdfsBaseDir + "/" + eloquaTenant.getId() + "/models/ANY_TABLE/"
                + eloquaModelId + "/container_01/";
        URL modelSummaryUrl = ClassLoader.getSystemResource(
                "com/latticeengines/pls/functionalframework/modelsummary-marketo.json");
        URL metadataDiagnosticsUrl = ClassLoader.getSystemResource(
                "com/latticeengines/pls/functionalframework/metadata-diagnostics.json");
        URL dataDiagnosticsUrl = ClassLoader
                .getSystemResource("com/latticeengines/pls/functionalframework/diagnostics.json");
        URL rfMoelUrl = ClassLoader
                .getSystemResource("com/latticeengines/pls/functionalframework/rf_model.txt");
        URL topPredictorUrl = ClassLoader.getSystemResource(
                "com/latticeengines/pls/functionalframework/topPredictor_model.csv");

        HdfsUtils.mkdir(yarnConfiguration, dir);
        HdfsUtils.mkdir(yarnConfiguration, dir + "/enhancements");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(),
                dir + "/enhancements/modelsummary.json");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, metadataDiagnosticsUrl.getFile(),
                dir + "/metadata-diagnostics.json");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, dataDiagnosticsUrl.getFile(),
                dir + "/diagnostics.json");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, rfMoelUrl.getFile(), dir + "/rf_model.txt");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, topPredictorUrl.getFile(),
                dir + "/topPredictor_model.csv");
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = "functional")
    public void loginToMarketoTenant_getModelSummariesForEloquaTenant_assertRetrievedModelSummariesCorrect() {
        setupSecurityContext(marketoTenant);
        List modelSummaries = restTemplate.getForObject(
                getRestAPIHostPort() + "/pls/modelsummaries/tenant/" + eloquaTenant.getName(),
                List.class);
        assertModelSummariesBelongToTenant(modelSummaries, eloquaTenant);
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = "functional")
    public void loginToEloquaTenant_getModelSummariesForEloquaTenant_assertRetrievedModelSummariesCorrect() {
        setupSecurityContext(eloquaTenant);
        List modelSummaries = restTemplate.getForObject(
                getRestAPIHostPort() + "/pls/modelsummaries/tenant/" + marketoTenant.getName(),
                List.class);
        assertModelSummariesBelongToTenant(modelSummaries, marketoTenant);

        modelSummaries = restTemplate.getForObject(
                getRestAPIHostPort() + "/pls/modelsummaries/tenant/" + eloquaTenant.getName(),
                List.class);
        assertModelSummariesBelongToTenant(modelSummaries, eloquaTenant);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void assertModelSummariesBelongToTenant(List modelSummaries, Tenant tenant) {
        for (Map modelSummary : (List<Map>) modelSummaries) {
            assertEquals(((Map) modelSummary.get("Tenant")).get("Identifier"), tenant.getId());
        }
    }
}
