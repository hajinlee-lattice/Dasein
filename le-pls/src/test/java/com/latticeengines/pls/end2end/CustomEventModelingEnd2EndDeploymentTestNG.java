package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.camille.exposed.config.ConfigurationController;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceScope;
import com.latticeengines.domain.exposed.encryption.EncryptionGlobalState;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.modeling.ModelingType;
import com.latticeengines.domain.exposed.modeling.factory.AlgorithmFactory;
import com.latticeengines.domain.exposed.modeling.factory.DataFlowFactory;
import com.latticeengines.domain.exposed.modeling.factory.SamplingFactory;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.security.Tenant;

public class CustomEventModelingEnd2EndDeploymentTestNG extends SelfServiceModelingEndToEndDeploymentTestNG {

    private static final Logger log = LoggerFactory.getLogger(CustomEventModelingEnd2EndDeploymentTestNG.class);

    private Tenant firstTenant;

    @BeforeClass(groups = { "deployment.cdl", "precheckin" })
    public void setup() throws Exception {
        log.info("Bootstrapping test tenants using tenant console ...");
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        firstTenant = testBed.getMainTestTenant();

        if (EncryptionGlobalState.isEnabled()) {
            ConfigurationController<CustomerSpaceScope> controller = ConfigurationController
                    .construct(new CustomerSpaceScope(CustomerSpace.parse(firstTenant.getId())));
            assertTrue(controller.exists(new Path("/EncryptionKey")));
        }

        // Create second tenant for copy model use case
        testBed.bootstrapForProduct(LatticeProduct.CG);

        // TODO: Set file name
        fileName = null;
        modelName = "CustomEventModelingEndToEndDeploymentTestNG_" + DateTime.now().getMillis();
        modelDisplayName = "Custom Event Modeling Test Display Name";
        schemaInterpretation = SchemaInterpretation.Account;

        // TODO: Add mocked Segment Data
        log.info("Test environment setup finished.");
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = { "deployment.cdl", "precheckin" })
    public void uploadFile() {
        // TODO: Select Target Segment & Set AiModel Id, RE Id?
        super.uploadFile();
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = { "deployment.cdl", "precheckin" }, dependsOnMethods = "uploadFile")
    public void resolveMetadata() {
        log.info("Resolving metadata for modeling ...");
        parameters = createModelingParameters();
        // TODO: Add some fieldmapping change

    }

    protected ModelingParameters createModelingParameters() {
        ModelingParameters parameters = new ModelingParameters();
        parameters.setName(modelName);
        parameters.setDisplayName(modelDisplayName);
        parameters.setDescription("Test");
        parameters.setFilename(sourceFile.getName());
        parameters.setActivateModelSummaryByDefault(true);
        Map<String, String> runtimeParams = new HashMap<>();
        runtimeParams.put(SamplingFactory.MODEL_SAMPLING_SEED_KEY, "987654");
        runtimeParams.put(AlgorithmFactory.RF_SEED_KEY, "987654");
        runtimeParams.put(DataFlowFactory.DATAFLOW_DO_SORT_FOR_ATTR_FLOW, "");
        parameters.setRunTimeParams(runtimeParams);
        parameters.setModelingType(ModelingType.CDL);

        // TODO: set segmentName
        return parameters;
    }

    @Override
    protected void inspectOriginalModelSummaryPredictors(ModelSummary modelSummary) {
        // Inspect some predictors
        String rawModelSummary = modelSummary.getDetails().getPayload();
        JsonNode modelSummaryJson = JsonUtils.deserialize(rawModelSummary, JsonNode.class);
        JsonNode predictors = modelSummaryJson.get("Predictors");
        for (int i = 0; i < predictors.size(); ++i) {
            JsonNode predictor = predictors.get(i);
            assertNotEquals(predictor.get("Name"), "Activity_Count_Interesting_Moment_Webinar");
            if (predictor.get("Name") != null && predictor.get("Name").asText() != null) {
                if (predictor.get("Name").asText().equals("Some_Column")) {
                    JsonNode tags = predictor.get("Tags");
                    assertEquals(tags.size(), 1);
                    assertEquals(tags.get(0).textValue(), ModelingMetadata.INTERNAL_TAG);
                    assertEquals(predictor.get("Category").textValue(), ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION);
                } else if (predictor.get("Name").asText().equals("Industry")) {
                    JsonNode approvedUsages = predictor.get("ApprovedUsage");
                    assertEquals(approvedUsages.size(), 1);
                    assertEquals(approvedUsages.get(0).textValue(), ApprovedUsage.MODEL_ALLINSIGHTS.toString());
                }
            }
        }
    }
}
