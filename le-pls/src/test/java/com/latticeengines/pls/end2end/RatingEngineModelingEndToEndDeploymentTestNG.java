package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.RatingEngineModelingParameters;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.pls.CloneModelingParameters;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.RatingEngineScoringParameters;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.AwsApsGeneratorUtils;
import com.latticeengines.domain.exposed.util.MetaDataTableUtils;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component
public class RatingEngineModelingEndToEndDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String RESOURCE_BASE = "com/latticeengines/pls/end2end/ratingEngineModeling";
    private static final Logger log = LoggerFactory.getLogger(RatingEngineModelingEndToEndDeploymentTestNG.class);
    public static final String MODEL_DISPLAY_NAME = "Rating Engine Modeling Test Display Name";
    protected com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy internalResourceRestApiProxy;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    protected MetadataProxy metadataProxy;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    private Configuration yarnConfiguration;

    private Tenant firstTenant;
    private String modelingWorkflowApplicationId;
    private String modelName;
    private ModelSummary originalModelSummary;

    private String trainFilterTableName = "TrainFilter";
    private String trainFilterFileName = "trainFilter.avro";
    private String eventFilterTableName = "EventFilter";
    private String eventFilterFileName = "eventFilter.avro";
    private String targetFilterTableName = "TargetFilter";
    private String targetFilterFileName = "targetFilter.avro";
    private String apsTableName = "AnalyticPurchaseState";
    private String apsFileName = "apsTable.avro";
    private String accountTableName = "AccountTable";
    private String accountFileName = "accountTable.avro";

    private RatingEngineModelingParameters modelingParameters;

    @BeforeClass(groups = { "deployment.lp" })
    public void setup() throws Exception {
        log.info("Bootstrapping test tenants using tenant console ...");
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        firstTenant = testBed.getMainTestTenant();

        modelingParameters = new RatingEngineModelingParameters();
        modelingParameters.setName("RatingEngineModelingEndToEndDeploymentTestNG_" + DateTime.now().getMillis());
        modelingParameters.setDisplayName(MODEL_DISPLAY_NAME);
        modelingParameters.setDescription("Test");
        modelingParameters.setModuleName("module");
        modelingParameters.setActivateModelSummaryByDefault(true);

        modelingParameters.setTrainFilterTableName(trainFilterTableName);
        modelingParameters.setEventFilterTableName(eventFilterTableName);
        modelingParameters.setTargetFilterTableName(targetFilterTableName);

        modelingParameters.setExpectedValue(false);
        modelingParameters.setLiftChart(true);

        log.info("Test environment setup finished.");
    }

    @Test(groups = { "deployment.lp" }, enabled = true)
    public void testWithTables() throws Exception {
        log.info("setting up tables for modeling...");
        setupTables();
        createModel();
        ModelSummary modelSummary = retrieveModelSummary();

        scoreWorkflow(modelSummary);

        cloneAndRemodel(modelSummary);
        retrieveModelSummaryForClonedModel();
    }

    private void scoreWorkflow(ModelSummary modelSummary) {
        RatingEngineScoringParameters scoringParameters = new RatingEngineScoringParameters();
        scoringParameters.setTableToScoreName(targetFilterTableName);
        scoringParameters.setSourceDisplayName("RatingEngineBulkScoring");
        scoringParameters.setExpectedValue(modelingParameters.isExpectedValue());
        scoringParameters.setLiftChart(modelingParameters.isLiftChart());

        String scoreApplicationId = restTemplate.postForObject(
                String.format("%s/pls/scores/rating/%s", getRestAPIHostPort(), modelSummary.getId()), //
                scoringParameters, String.class);
        scoreApplicationId = StringUtils.substringBetween(scoreApplicationId.split(":")[1], "\"");
        System.out.println(String.format("Score rating data applicationId = %s", scoreApplicationId));
        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, scoreApplicationId, false);
        assertEquals(completedStatus, JobStatus.COMPLETED);

    }

    @Test(groups = { "deployment.lp" }, enabled = false)
    public void testWithQueries() throws Exception {
        String prodId = "A78DF03BAC196BE9A08508FFDB433A31";
        Bucket.Transaction txn = new Bucket.Transaction(prodId, TimeFilter.ever(), null, null, false);
        EventFrontEndQuery query = getQuery(txn);
        modelingParameters.setTrainFilterQuery(query);
        modelingParameters.setEventFilterQuery(query);
        modelingParameters.setTargetFilterQuery(query);
        modelingParameters.setTrainFilterTableName(null);
        modelingParameters.setEventFilterTableName(null);
        modelingParameters.setTargetFilterTableName(null);
        createModel();
        retrieveModelSummary();
    }

    private void setupTables() throws IOException {
        CustomerSpace customerSpace = CustomerSpace.parse(firstTenant.getName());
        setupTable(customerSpace, trainFilterFileName, trainFilterTableName);
        setupTable(customerSpace, eventFilterFileName, eventFilterTableName);
        setupTable(customerSpace, targetFilterFileName, targetFilterTableName);
        setupTable(customerSpace, apsFileName, apsTableName);
        Table accountTable = setupTable(customerSpace, accountFileName, accountTableName);

        DataCollection.Version version = dataCollectionProxy.getActiveVersion(customerSpace.toString());
        dataCollectionProxy.upsertTable(customerSpace.toString(), apsTableName, //
                TableRoleInCollection.AnalyticPurchaseState, version);
        dataCollectionProxy.upsertTable(customerSpace.toString(), accountTable.getName(), //
                TableRoleInCollection.ConsolidatedAccount, version);
    }

    private EventFrontEndQuery getQuery(Bucket.Transaction txn) {
        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.Transaction, "AnyThing");
        EventFrontEndQuery frontEndQuery = new EventFrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Bucket bucket = Bucket.txnBkt(txn);
        Restriction restriction = new BucketRestriction(attrLookup, bucket);
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setPageFilter(new PageFilter(0, 0));
        return frontEndQuery;
    }

    private Table setupTable(CustomerSpace customerSpace, String fileName, String tableName) throws IOException {
        String hdfsDir = PathBuilder
                .buildDataTablePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(firstTenant.getName()))
                .toString();
        if (!HdfsUtils.fileExists(yarnConfiguration, hdfsDir)) {
            HdfsUtils.mkdir(yarnConfiguration, hdfsDir);
        }
        hdfsDir += "/" + tableName;
        String fullLocalPath = RESOURCE_BASE + "/" + fileName;
        InputStream fileStream = ClassLoader.getSystemResourceAsStream(fullLocalPath);
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, fileStream, hdfsDir + "/" + fileName);

        Table table = MetaDataTableUtils.createTable(yarnConfiguration, tableName, hdfsDir);
        table.getExtracts().get(0).setExtractionTimestamp(System.currentTimeMillis());
        if (tableName.equals(apsTableName)) {
            AwsApsGeneratorUtils.setupMetaData(table);
        }
        metadataProxy.updateTable(customerSpace.toString(), tableName, table);
        return table;
    }

    public void createModel() {
        modelName = modelingParameters.getName();
        log.info("modelName:" + modelName);
        model(modelingParameters);
    }

    @SuppressWarnings("rawtypes")
    private void model(ModelingParameters parameters) {
        log.info("Start modeling ...");
        ResponseDocument response;
        String url = String.format("%s/pls/models/rating/%s", getRestAPIHostPort(), parameters.getName());
        System.out.println("json=" + JsonUtils.serialize(parameters));
        response = restTemplate.postForObject(url, parameters, ResponseDocument.class);
        modelingWorkflowApplicationId = new ObjectMapper().convertValue(response.getResult(), String.class);

        log.info(String.format("Workflow application id is %s", modelingWorkflowApplicationId));

        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, modelingWorkflowApplicationId, false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    void cloneAndRemodel(ModelSummary baseModelSummary) {
        log.info("Cloning and remodel the model summary ...");
        @SuppressWarnings("unchecked")
        List<Object> rawFields = restTemplate.getForObject(
                String.format("%s/pls/modelsummaries/metadata/%s", getRestAPIHostPort(), baseModelSummary.getId()),
                List.class);
        List<VdbMetadataField> fields = new ArrayList<>();
        for (Object rawField : rawFields) {
            VdbMetadataField field = JsonUtils.convertValue(rawField, VdbMetadataField.class);
            fields.add(field);
            if (field.getColumnName().equals("EMPLOYEES_HERE_RELIABILITY_CODE")) {
                field.setApprovedUsage(ModelingMetadata.NONE_APPROVED_USAGE);
            }
            if (field.getColumnName().equals("IMPORT_EXPORT_AGENT_CODE")) {
                field.setApprovedUsage(ModelingMetadata.NONE_APPROVED_USAGE);
            }
        }

        // Now remodel
        CloneModelingParameters parameters = new CloneModelingParameters();
        parameters.setName(modelName + "_clone");
        modelName = parameters.getName();
        parameters.setDisplayName(MODEL_DISPLAY_NAME);
        parameters.setDescription("clone");
        parameters.setAttributes(fields);
        parameters.setSourceModelSummaryId(baseModelSummary.getId());
        parameters.setDeduplicationType(DedupType.MULTIPLELEADSPERDOMAIN);
        parameters.setEnableTransformations(false);
        parameters.setExcludePropDataAttributes(false);
        parameters.setActivateModelSummaryByDefault(true);
        ResponseDocument<?> response;
        response = restTemplate.postForObject(
                String.format("%s/pls/models/rating/%s/clone", getRestAPIHostPort(), modelName), parameters,
                ResponseDocument.class);
        modelingWorkflowApplicationId = new ObjectMapper().convertValue(response.getResult(), String.class);
        log.info(String.format("Workflow application id is %s", modelingWorkflowApplicationId));
        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, modelingWorkflowApplicationId, false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    public void retrieveModelSummaryForClonedModel() throws InterruptedException, IOException {
        log.info("Retrieve the model summary after cloning and remodeling ...");
        ModelSummary clonedModelSummary = waitToDownloadModelSummary(modelName);
        assertNotNull(clonedModelSummary);
        List<Predictor> predictors = clonedModelSummary.getPredictors();
        assertTrue(!Iterables.any(predictors, new Predicate<Predictor>() {
            @Override
            public boolean apply(@Nullable Predictor predictor) {
                return predictor.getName().equals(InterfaceName.Website.toString())
                        || predictor.getName().equals(InterfaceName.Country.toString());
            }

        }));
        assertEquals(clonedModelSummary.getSourceSchemaInterpretation(),
                SchemaInterpretation.SalesforceAccount.toString());
        // assertEquals(clonedModelSummary.getModelSummaryConfiguration()
        // .getString(ProvenancePropertyName.TransformationGroupName, null),
        // TransformationGroup.ALL.getName());
        String trainingTableName = clonedModelSummary.getTrainingTableName();
        assertNotNull(trainingTableName);
        String targetTableName = clonedModelSummary.getTargetTableName();
        assertEquals(trainingTableName + "_TargetTable", targetTableName);

        @SuppressWarnings("unchecked")
        List<Object> rawFields = restTemplate.getForObject(
                String.format("%s/pls/modelsummaries/metadata/%s", getRestAPIHostPort(), clonedModelSummary.getId()),
                List.class);
        assertTrue(Iterables.any(rawFields, new Predicate<Object>() {
            @Override
            public boolean apply(@Nullable Object raw) {
                VdbMetadataField metadataField = new ObjectMapper().convertValue(raw, VdbMetadataField.class);
                return metadataField.getColumnName().equals("EMPLOYEES_HERE_RELIABILITY_CODE")
                        && metadataField.getApprovedUsage().equals(ModelingMetadata.NONE_APPROVED_USAGE);
            }
        }));
        assertTrue(Iterables.any(rawFields, new Predicate<Object>() {
            @Override
            public boolean apply(@Nullable Object raw) {
                VdbMetadataField metadataField = new ObjectMapper().convertValue(raw, VdbMetadataField.class);
                return metadataField.getColumnName().equals("IMPORT_EXPORT_AGENT_CODE")
                        && metadataField.getApprovedUsage().equals(ModelingMetadata.NONE_APPROVED_USAGE);
            }
        }));
    }

    public ModelSummary retrieveModelSummary() throws InterruptedException {
        log.info("Retrieving model summary for modeling ...");
        originalModelSummary = waitToDownloadModelSummary(modelName);
        assertNotNull(originalModelSummary);
        assertEquals(originalModelSummary.getSourceSchemaInterpretation(),
                SchemaInterpretation.SalesforceAccount.toString());
        assertNotNull(originalModelSummary.getTrainingTableName());
        assertFalse(originalModelSummary.getTrainingTableName().isEmpty());
        inspectOriginalModelSummaryPredictors(originalModelSummary);
        return originalModelSummary;
    }

    ModelSummary waitToDownloadModelSummary(String modelName) throws InterruptedException {
        log.info(String.format("Getting the model whose name contains %s", modelName));
        ModelSummary found = null;
        while (true) {
            @SuppressWarnings("unchecked")
            List<Object> summaries = restTemplate.getForObject( //
                    String.format("%s/pls/modelsummaries", getRestAPIHostPort()), List.class);
            for (Object rawSummary : summaries) {
                ModelSummary summary = new ObjectMapper().convertValue(rawSummary, ModelSummary.class);
                if (summary.getName().contains(modelName)) {
                    found = summary;
                }
            }
            log.info(String.format("Getting the model whose name contains %s", modelName));
            if (found != null)
                break;
            Thread.sleep(1000);
        }
        assertNotNull(found);

        @SuppressWarnings("unchecked")
        List<Object> predictors = restTemplate.getForObject(
                String.format("%s/pls/modelsummaries/predictors/all/%s", getRestAPIHostPort(), found.getId()),
                List.class);
        assertTrue(predictors.stream().anyMatch(x -> {
            Predictor predictor = new ObjectMapper().convertValue(x, Predictor.class);
            return predictor.getCategory() != null;
        }));

        // Look up the model summary with details
        Object rawSummary = restTemplate.getForObject(
                String.format("%s/pls/modelsummaries/%s", getRestAPIHostPort(), found.getId()), Object.class);
        return JsonUtils.convertValue(rawSummary, ModelSummary.class);
    }

    private void inspectOriginalModelSummaryPredictors(ModelSummary modelSummary) {
        // Inspect some predictors
        String rawModelSummary = modelSummary.getDetails().getPayload();
        JsonNode modelSummaryJson = JsonUtils.deserialize(rawModelSummary, JsonNode.class);
        JsonNode predictors = modelSummaryJson.get("Predictors");
        for (int i = 0; i < predictors.size(); ++i) {
            JsonNode predictor = predictors.get(i);
            assertNotEquals(predictor.get("Name"),
                    "Activity_Count_Interesting_Moment_Webinar" + "ty_Count_Interesting_Moment_Webinar");
            if (predictor.get("Name") != null && predictor.get("Name").asText() != null) {
                if (predictor.get("Name").asText().equals("LE_EMPLOYEE_RANGE")) {
                    JsonNode tags = predictor.get("Tags");
                    assertEquals(tags.size(), 1);
                    assertEquals(tags.get(0).textValue(), ModelingMetadata.EXTERNAL_TAG);
                    assertEquals(predictor.get("Category").textValue(), ModelingMetadata.CATEGORY_FIRMOGRAPHICS);
                } else if (predictor.get("Name").asText()
                        .equals("Product_2A2A5856EC1CCB78E786DF65564DA39E_RevenueRollingSum6")) {
                    JsonNode approvedUsages = predictor.get("ApprovedUsage");
                    assertEquals(approvedUsages.size(), 1);
                    assertEquals(approvedUsages.get(0).textValue(), ApprovedUsage.MODEL_ALLINSIGHTS.toString());
                    JsonNode tags = predictor.get("Tags");
                    assertEquals(tags.get(0).textValue(), ModelingMetadata.INTERNAL_TAG);
                }
            }
        }
    }

}
