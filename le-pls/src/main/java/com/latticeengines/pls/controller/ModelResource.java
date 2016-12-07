package com.latticeengines.pls.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.util.NameValidationUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modelreview.ColumnRuleResult;
import com.latticeengines.domain.exposed.modelreview.ModelReviewData;
import com.latticeengines.domain.exposed.modelreview.ModelReviewDataRule;
import com.latticeengines.domain.exposed.modelreview.RowRuleResult;
import com.latticeengines.domain.exposed.pls.CloneModelingParameters;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryDownloadFlagEntityMgr;
import com.latticeengines.pls.service.ModelCopyService;
import com.latticeengines.pls.service.ModelMetadataService;
import com.latticeengines.pls.service.ModelReplaceService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.workflow.ImportMatchAndModelWorkflowSubmitter;
import com.latticeengines.pls.workflow.MatchAndModelWorkflowSubmitter;
import com.latticeengines.pls.workflow.PMMLModelWorkflowSubmitter;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

import edu.emory.mathcs.backport.java.util.Arrays;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "models", description = "REST resource for interacting with modeling workflows")
@RestController
@RequestMapping("/models")
@PreAuthorize("hasRole('View_PLS_Data')")
public class ModelResource {
    private static final Logger log = Logger.getLogger(ModelResource.class);

    @Autowired
    private ImportMatchAndModelWorkflowSubmitter importMatchAndModelWorkflowSubmitter;

    @Autowired
    private MatchAndModelWorkflowSubmitter modelWorkflowSubmitter;

    @Autowired
    private ModelSummaryService modelSummaryService;

    @Autowired
    private ModelMetadataService modelMetadataService;

    @Autowired
    private PMMLModelWorkflowSubmitter pmmlModelWorkflowSubmitter;

    @Autowired
    private ModelCopyService modelCopyService;

    @Autowired
    private ModelReplaceService modelReplaceService;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private ModelSummaryDownloadFlagEntityMgr modelSummaryDownloadFlagEntityMgr;

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Value("${common.test.microservice.url}")
    private String microserviceEndpoint;

    @RequestMapping(value = "/{modelName}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Generate a model from the supplied file and parameters. Returns the job id.")
    public ResponseDocument<String> model(@PathVariable String modelName, //
            @RequestBody ModelingParameters parameters) {
        if (!NameValidationUtils.validateModelName(modelName)) {
            String message = String.format(
                    "Not qualified modelName %s contains unsupported characters.", modelName);
            log.error(message);
            throw new RuntimeException(message);
        }
        modelSummaryDownloadFlagEntityMgr.addDownloadFlag(MultiTenantContext.getTenant().getId());
        log.info(String.format("model called with parameters %s", parameters.toString()));
        return ResponseDocument.successResponse( //
                importMatchAndModelWorkflowSubmitter.submit(parameters).toString());

    }

    @RequestMapping(value = "/{modelName}/clone", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Clones and remodels with the specified model name.")
    @PreAuthorize("hasRole('Edit_PLS_Refine_Clone')")
    public ResponseDocument<String> cloneAndRemodel(@PathVariable String modelName,
            @RequestBody CloneModelingParameters parameters) {
        log.info(String.format("cloneAndRemodel called with parameters %s, dedupOption: %s",
                parameters.toString(), parameters.getDeduplicationType()));
        Table clone = modelMetadataService.cloneTrainingTable(parameters.getSourceModelSummaryId());

        ModelSummary modelSummary = modelSummaryService
                .getModelSummaryEnrichedByDetails(parameters.getSourceModelSummaryId());

        SourceFile sourceFile = sourceFileService
                .findByTableName(modelSummary.getTrainingTableName());
        if (sourceFile != null) {
            sourceFileService.copySourceFile(clone.getName(), sourceFile,
                    tenantEntityMgr.findByTenantId(MultiTenantContext.getTenant().getId()));
        } else {
            log.warn("Unable to find source file for model summary:" + modelSummary.getName());
        }

        Table parentModelEventTable = metadataProxy.getTable(MultiTenantContext.getTenant().getId(),
                modelSummary.getEventTableName());
        List<Attribute> userRefinedAttributes = modelMetadataService.getAttributesFromFields(
                parentModelEventTable.getAttributes(), parameters.getAttributes());
        modelSummaryDownloadFlagEntityMgr.addDownloadFlag(MultiTenantContext.getTenant().getId());
        return ResponseDocument.successResponse( //
                modelWorkflowSubmitter
                        .submit(clone.getName(), parameters, userRefinedAttributes, modelSummary)
                        .toString());
    }

    @RequestMapping(value = "/pmml/{modelName}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Generate a PMML model from the supplied module. Returns the job id.")
    public ResponseDocument<String> modelForPmml(@PathVariable String modelName, //
            @RequestParam(value = "displayname") String modelDisplayName, //
            @RequestParam(value = "module") String moduleName, //
            @RequestParam(value = "pivotfile", required = false) String pivotFileName, //
            @RequestParam(value = "pmmlfile") String pmmlFileName,
            @RequestParam(value = "schema") SchemaInterpretation schemaInterpretation) {
        if (!NameValidationUtils.validateModelName(modelName)) {
            String message = String.format(
                    "Not qualified modelName %s contains unsupported characters.", modelName);
            log.error(message);
            throw new RuntimeException(message);
        }
        modelSummaryDownloadFlagEntityMgr.addDownloadFlag(MultiTenantContext.getTenant().getId());
        String appId = pmmlModelWorkflowSubmitter.submit(modelName, modelDisplayName, moduleName,
                pivotFileName, pmmlFileName, schemaInterpretation).toString();
        return ResponseDocument.successResponse(appId);

    }

    @RequestMapping(value = "/copymodel/{modelId}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Copy a model from current tenant to target tenant.")
    public ResponseDocument<Boolean> copyModel(@PathVariable String modelId,
            @RequestParam(value = "targetTenantId") String targetTenantId) {
        modelSummaryDownloadFlagEntityMgr.addDownloadFlag(targetTenantId);
        return ResponseDocument.successResponse( //
                modelCopyService.copyModel(targetTenantId, modelId));
    }

    @RequestMapping(value = "/replacemodel/{sourceModelId}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Use source tenant's model to replace target tenant's model.")
    public ResponseDocument<Boolean> replaceModel(@PathVariable String sourceModelId,
            @RequestParam(value = "targetTenantId") String targetTenantId,
            @RequestParam(value = "targetModelId") String targetModelId) {
        modelSummaryDownloadFlagEntityMgr.addDownloadFlag(targetTenantId);
        return ResponseDocument.successResponse( //
                modelReplaceService.replaceModel(sourceModelId, targetTenantId, targetModelId));
    }

    @RequestMapping(value = "/reviewmodel/{modelName}/{eventTableName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the model review data rules and rule output for the model")
    public ResponseDocument<ModelReviewData> getModelReviewData(@PathVariable String modelName,
            @PathVariable String eventTableName) throws IOException {
        Tenant tenant = MultiTenantContext.getTenant();
        return ResponseDocument.successResponse(
                metadataProxy.getReviewData(tenant.getId(), modelName, eventTableName));
    }

    @RequestMapping(value = "/modelreview/mocked/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the data rules for model")
    public ResponseDocument<List<ModelReviewDataRule>> getModelDataRules(
            @PathVariable String modelId) throws IOException {
        return ResponseDocument.successResponse(generateMockedDataRules());
    }

    @RequestMapping(value = "/modelreview/attributes/mocked/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get customer provided attributes for model")
    public ResponseDocument<List<VdbMetadataField>> getCustomModelAttributes(
            @PathVariable String modelId) {
        return ResponseDocument.successResponse(generateMockedAttributes());
    }

    @RequestMapping(value = "/reviewmodel/column", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Create the column results")
    public ResponseDocument<Boolean> createModelColumnResults(
            @RequestBody List<ColumnRuleResult> columnRuleResults) {
        return ResponseDocument
                .successResponse(metadataProxy.createColumnResults(columnRuleResults));
    }

    @RequestMapping(value = "/reviewmodel/row", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Create the row results")
    public ResponseDocument<Boolean> createModelRowResults(
            @RequestBody List<RowRuleResult> rowRuleResults) {
        return ResponseDocument.successResponse(metadataProxy.createRowResults(rowRuleResults));
    }

    @RequestMapping(value = "/reviewmodel/column/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the column results")
    public ResponseDocument<List<ColumnRuleResult>> getColumnRuleResults(
            @PathVariable String modelId) {
        return ResponseDocument.successResponse(metadataProxy.getColumnResults(modelId));
    }

    @RequestMapping(value = "/reviewmodel/row/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the row results")
    public ResponseDocument<List<RowRuleResult>> getRowRuleResults(@PathVariable String modelId) {
        return ResponseDocument.successResponse(metadataProxy.getRowResults(modelId));
    }

    @SuppressWarnings("unchecked")
    private List<VdbMetadataField> generateMockedAttributes() {
        VdbMetadataField METADATA_1 = new VdbMetadataField();
        METADATA_1.setColumnName("AnnualRevenue");
        METADATA_1.setDisplayName("AnnualRevenue");
        METADATA_1.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS.toString());
        METADATA_1.setTags("Internal");
        METADATA_1.setAssociatedRules(new ArrayList<String>());

        VdbMetadataField METADATA_2 = new VdbMetadataField();
        METADATA_2.setColumnName("BusinessTechnologiesSeoTitle");
        METADATA_2.setDisplayName("BusinessTechnologiesSeoTitle");
        METADATA_2.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS.toString());
        METADATA_2.setTags("InternalTransform");
        METADATA_2.setAssociatedRules(new ArrayList<String>());

        VdbMetadataField METADATA_3 = new VdbMetadataField();
        METADATA_3.setColumnName("City");
        METADATA_3.setDisplayName("City");
        METADATA_3.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS.toString());
        METADATA_3.setTags("Internal");
        METADATA_3.setAssociatedRules(new ArrayList<String>());

        VdbMetadataField METADATA_4 = new VdbMetadataField();
        METADATA_4.setColumnName("CompanyType");
        METADATA_4.setDisplayName("CompanyType");
        METADATA_4.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS.toString());
        METADATA_4.setTags("Internal");
        METADATA_4.setIsCoveredByOptionalRule(true);
        METADATA_4.setAssociatedRules(Arrays.asList(new String[] { "ModelBias" }));

        VdbMetadataField METADATA_5 = new VdbMetadataField();
        METADATA_5.setColumnName("Country");
        METADATA_5.setDisplayName("Country");
        METADATA_5.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS.toString());
        METADATA_5.setTags("Internal");
        METADATA_5.setAssociatedRules(new ArrayList<String>());

        VdbMetadataField METADATA_6 = new VdbMetadataField();
        METADATA_6.setColumnName("City");
        METADATA_6.setDisplayName("City");
        METADATA_6.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS.toString());
        METADATA_6.setTags("Internal");
        METADATA_6.setAssociatedRules(new ArrayList<String>());

        VdbMetadataField METADATA_7 = new VdbMetadataField();
        METADATA_7.setColumnName("NumberOfEmployees");
        METADATA_7.setDisplayName("NumberOfEmployees");
        METADATA_7.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS.toString());
        METADATA_7.setTags("Internal");
        METADATA_7.setIsCoveredByOptionalRule(true);
        METADATA_7.setIsCoveredByMandatoryRule(true);
        METADATA_7.setAssociatedRules(Arrays.asList(new String[] { "TooManyValues" }));

        VdbMetadataField METADATA_8 = new VdbMetadataField();
        METADATA_8.setColumnName("CompanyType");
        METADATA_8.setDisplayName("CompanyType");
        METADATA_8.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS.toString());
        METADATA_8.setTags("Internal");
        METADATA_8.setIsCoveredByOptionalRule(true);
        METADATA_8.setAssociatedRules(
                Arrays.asList(new String[] { "LowCoverage", "MissingPredictiveValues" }));

        return Arrays.asList(new VdbMetadataField[] { METADATA_1, METADATA_2, METADATA_3,
                METADATA_4, METADATA_5, METADATA_6, METADATA_7, METADATA_8 });
    }

    @SuppressWarnings("unchecked")
    private List<ModelReviewDataRule> generateMockedDataRules() {
        ModelReviewDataRule DATA_RULE_1 = new ModelReviewDataRule();
        DATA_RULE_1.setName("LowCoverage");
        DATA_RULE_1.setDisplayName("Low Coverage");
        DATA_RULE_1.setDescription(
                "This attribute is missing value for more than 98% of records, which causes scores to be less accurate");
        DATA_RULE_1.setColumnsToRemediate(Arrays.asList(new String[] { "NumnberOfOffices" }));

        ModelReviewDataRule DATA_RULE_2 = new ModelReviewDataRule();
        DATA_RULE_2.setName("MissingPredictiveValues");
        DATA_RULE_2.setDisplayName("Missing Values are predictive");
        DATA_RULE_2.setDescription(
                "When this attribute is missing a value, it is more likely to convert. This often caues...");
        DATA_RULE_2.setColumnsToRemediate(Arrays.asList(new String[] { "NumberOfOffices" }));

        ModelReviewDataRule DATA_RULE_3 = new ModelReviewDataRule();
        DATA_RULE_3.setName("ModelBias");
        DATA_RULE_3.setDisplayName("Model Bias");
        DATA_RULE_3.setDescription(
                "This attribute is introducing bias into the model. This mean it will more than look like it's improving the model during training");
        DATA_RULE_3.setColumnsToRemediate(Arrays.asList(new String[] { "CompanyType" }));

        ModelReviewDataRule DATA_RULE_4 = new ModelReviewDataRule();
        DATA_RULE_4.setName("TooManyValues");
        DATA_RULE_4.setDisplayName("Too many values");
        DATA_RULE_4.setDescription(
                "This text attribute cannot be included in models because it has too many values, which causes scores to be less accurate. For text attributes, ...");
        DATA_RULE_4.setColumnsToRemediate(Arrays.asList(new String[] { "NumberOfEmployees" }));
        DATA_RULE_4.setIsMandatory(true);

        return Arrays.asList(
                new ModelReviewDataRule[] { DATA_RULE_1, DATA_RULE_2, DATA_RULE_3, DATA_RULE_4 });
    }

}
