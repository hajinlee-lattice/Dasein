package com.latticeengines.pls.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.NameValidationUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CrossSellModelingParameters;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.modelreview.ColumnRuleResult;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.modelreview.ModelReviewData;
import com.latticeengines.domain.exposed.modelreview.RowRuleResult;
import com.latticeengines.domain.exposed.pls.CloneModelingParameters;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.pls.service.ModelMetadataService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.workflow.MatchAndModelWorkflowSubmitter;
import com.latticeengines.pls.workflow.PMMLModelWorkflowSubmitter;
import com.latticeengines.proxy.exposed.cdl.CDLModelProxy;
import com.latticeengines.proxy.exposed.lp.ModelCopyProxy;
import com.latticeengines.proxy.exposed.lp.ModelOperationProxy;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "models", description = "REST resource for interacting with modeling workflows")
@RestController
@RequestMapping("/models")
@PreAuthorize("hasRole('View_PLS_Data')")
public class ModelResource {
    private static final Logger log = LoggerFactory.getLogger(ModelResource.class);

    @Inject
    private MatchAndModelWorkflowSubmitter modelWorkflowSubmitter;

    @Inject
    private ModelMetadataService modelMetadataService;

    @Inject
    private PMMLModelWorkflowSubmitter pmmlModelWorkflowSubmitter;

    @Inject
    private ModelCopyProxy modelCopyProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Inject
    private SourceFileService sourceFileService;

    @Inject
    private ModelOperationProxy modelOperationProxy;

    @Inject
    private CDLModelProxy cdlModelProxy;

    @Value("${common.test.microservice.url}")
    private String microserviceEndpoint;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    @Value("${camille.zk.pod.id:Default}")
    private String podId;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Resource(name = "distCpConfiguration")
    private Configuration distCpConfiguration;

    @RequestMapping(value = "/{modelName}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Generate a model from the supplied file and parameters. Returns the job id.")
    public ResponseDocument<String> model(@PathVariable String modelName, //
            @RequestBody ModelingParameters parameters) {
        parameters.setUserId(MultiTenantContext.getEmailAddress());
        return ResponseDocument
                .successResponse(cdlModelProxy.model(MultiTenantContext.getTenant().getId(), modelName, parameters));
    }

    @RequestMapping(value = "/rating/{modelName}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Generate a cross sell model from the table name(or query) and parameters. Returns the job id.")
    public ResponseDocument<String> crossSellModel(@PathVariable String modelName, //
            @RequestBody CrossSellModelingParameters parameters) {
        parameters.setUserId(MultiTenantContext.getEmailAddress());
        try {
            return ResponseDocument.successResponse(
                    cdlModelProxy.model(MultiTenantContext.getTenant().getId(), modelName, parameters));
        } catch (Exception ex) {
            log.error("Modeling job failed!", ex);
            throw new RuntimeException("Modeling job failed, contact Lattice support for details!");
        }
    }

    @RequestMapping(value = "/rating/{modelName}/clone", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Clones and remodels with the specified model name.")
    public ResponseDocument<String> ratingEngineCloneAndRemodel(@PathVariable String modelName,
            @RequestBody CloneModelingParameters parameters) {

        try {
            parameters.setUserId(MultiTenantContext.getEmailAddress());
            return ResponseDocument.successResponse(
                    cdlModelProxy.clone(MultiTenantContext.getTenant().getId(), modelName, parameters));
        } catch (Exception ex) {
            log.error("Modeling job failed!", ex);
            throw new RuntimeException("Modeling job failed, contact Lattice support for details!");
        }
    }

    @RequestMapping(value = "/{modelName}/clone", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Clones and remodels with the specified model name.")
    @PreAuthorize("hasRole('Edit_PLS_Refine_Clone')")
    public ResponseDocument<String> cloneAndRemodel(@PathVariable String modelName,
            @RequestBody CloneModelingParameters parameters) {
        if (!NameValidationUtils.validateModelName(modelName)) {
            String message = String.format("Not qualified modelName %s contains unsupported characters.", modelName);
            log.error(message);
            throw new RuntimeException(message);
        }

        ModelSummary modelSummary = modelSummaryProxy.getModelSummaryEnrichedByDetails(
                MultiTenantContext.getTenant().getId(), parameters.getSourceModelSummaryId());
        Table trainingTable = metadataProxy.getTable(MultiTenantContext.getTenant().getId(),
                modelSummary.getTrainingTableName());

        importFromS3IfNeeded(trainingTable, modelSummary);

        log.info(String.format("cloneAndRemodel called with parameters %s, dedupOption: %s", parameters.toString(),
                parameters.getDeduplicationType()));
        Table clone = modelCopyProxy.cloneTrainingTable(MultiTenantContext.getShortTenantId(),
                parameters.getSourceModelSummaryId());

        SourceFile sourceFile = sourceFileService.findByTableName(modelSummary.getTrainingTableName());
        if (sourceFile != null) {
            sourceFileService.copySourceFile(sourceFile.getName(), clone.getName(),
                    MultiTenantContext.getShortTenantId());
        } else {
            log.warn("Unable to find source file for model summary:" + modelSummary.getName());
        }

        Table parentModelEventTable = metadataProxy.getTable(MultiTenantContext.getTenant().getId(),
                modelSummary.getEventTableName());
        List<Attribute> userRefinedAttributes = modelMetadataService
                .getAttributesFromFields(parentModelEventTable.getAttributes(), parameters.getAttributes());
        modelSummaryProxy.setDownloadFlag(MultiTenantContext.getTenant().getId());
        parameters.setUserId(MultiTenantContext.getEmailAddress());
        return ResponseDocument.successResponse( //
                modelWorkflowSubmitter.submit(clone.getName(), parameters, userRefinedAttributes, modelSummary)
                        .toString());
    }

    private void importFromS3IfNeeded(Table trainigTable, ModelSummary modelSummary) {
        String tenantId = MultiTenantContext.getTenant().getId();
        tenantId = CustomerSpace.parse(tenantId).getTenantId();
        String pivotFilePath = modelSummary.getPivotArtifactPath();
        try {
            String protocol = Boolean.TRUE.equals(useEmr) ? "s3a" : "s3n";
            HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder(protocol);
            if (StringUtils.isNotBlank(pivotFilePath) && !HdfsUtils.fileExists(distCpConfiguration, pivotFilePath)) {
                String s3Path = builder.convertAtlasMetadata(pivotFilePath, podId, tenantId, s3Bucket);
                HdfsUtils.copyFiles(distCpConfiguration, s3Path, pivotFilePath);
            }
        } catch (Exception ex) {
            String msg = "Failed to copy file=" + pivotFilePath + " tenantId=" + tenantId;
            log.error(msg, ex);
            throw new RuntimeException(ex);
        }
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
            String message = String.format("Not qualified modelName %s contains unsupported characters.", modelName);
            log.error(message);
            throw new RuntimeException(message);
        }
        modelSummaryProxy.setDownloadFlag(MultiTenantContext.getTenant().getId());
        String appId = pmmlModelWorkflowSubmitter
                .submit(modelName, modelDisplayName, moduleName, pivotFileName, pmmlFileName, schemaInterpretation)
                .toString();
        return ResponseDocument.successResponse(appId);

    }

    @RequestMapping(value = "/copymodel/{modelId}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Copy a model from current tenant to target tenant.")
    public ResponseDocument<Boolean> copyModel(@PathVariable String modelId,
            @RequestParam(value = "targetTenantId") String targetTenantId) {
        modelSummaryProxy.setDownloadFlag(MultiTenantContext.getTenant().getId());
        modelCopyProxy.copyModel(MultiTenantContext.getShortTenantId(), targetTenantId, modelId);
        return ResponseDocument.successResponse(true);
    }

    @RequestMapping(value = "/replacemodel/{sourceModelId}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Use source tenant's model to replace target tenant's model.")
    public ResponseDocument<Boolean> replaceModel(@PathVariable String sourceModelId,
            @RequestParam(value = "targetTenantId") String targetTenantId,
            @RequestParam(value = "targetModelId") String targetModelId) {
        modelSummaryProxy.setDownloadFlag(MultiTenantContext.getTenant().getId());
        return ResponseDocument.successResponse( //
                modelOperationProxy.replaceModel(MultiTenantContext.getShortTenantId(), sourceModelId, targetTenantId,
                        targetModelId));
    }

    @RequestMapping(value = "/reviewmodel/{modelName}/{eventTableName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the model review data rules and rule output for the model")
    public ResponseDocument<ModelReviewData> getModelReviewData(@PathVariable String modelName,
            @PathVariable String eventTableName) throws IOException {
        Tenant tenant = MultiTenantContext.getTenant();
        return ResponseDocument.successResponse(metadataProxy.getReviewData(tenant.getId(), modelName, eventTableName));
    }

    @RequestMapping(value = "/modelreview/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the data rules for model")
    public ResponseDocument<List<DataRule>> getDataRules(@PathVariable String modelId) throws IOException {
        return ResponseDocument.successResponse(modelMetadataService.getEventTableFromModelId(modelId).getDataRules());
    }

    @RequestMapping(value = "/modelreview/attributes/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get customer provided attributes for model")
    public ResponseDocument<List<VdbMetadataField>> getModelAttributes(@PathVariable String modelId) {
        ModelSummary modelSummary = modelSummaryProxy.getByModelId(modelId);
        return ResponseDocument
                .successResponse(filterAttributesForModelReview(modelMetadataService.getMetadata(modelId),
                        SchemaInterpretation.valueOf(modelSummary.getSourceSchemaInterpretation())));
    }

    @RequestMapping(value = "/reviewmodel/column", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Create the column results")
    public ResponseDocument<Boolean> createModelColumnResults(@RequestBody List<ColumnRuleResult> columnRuleResults) {
        return ResponseDocument.successResponse(metadataProxy.createColumnResults(columnRuleResults));
    }

    @RequestMapping(value = "/reviewmodel/row", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Create the row results")
    public ResponseDocument<Boolean> createModelRowResults(@RequestBody List<RowRuleResult> rowRuleResults) {
        return ResponseDocument.successResponse(metadataProxy.createRowResults(rowRuleResults));
    }

    @RequestMapping(value = "/reviewmodel/column/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the column results")
    public ResponseDocument<List<ColumnRuleResult>> getColumnRuleResults(@PathVariable String modelId) {
        return ResponseDocument.successResponse(metadataProxy.getColumnResults(modelId));
    }

    @RequestMapping(value = "/reviewmodel/row/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the row results")
    public ResponseDocument<List<RowRuleResult>> getRowRuleResults(@PathVariable String modelId) {
        return ResponseDocument.successResponse(metadataProxy.getRowResults(modelId));
    }

    @RequestMapping(value = "/cleanup/{modelId}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Clean up model")
    public ResponseDocument<Boolean> cleanUpModel(@PathVariable String modelId) {
        log.info("Clean up model by user: " + MultiTenantContext.getEmailAddress());
        return ResponseDocument.successResponse(modelOperationProxy.cleanUpModel(modelId));
    }

    private List<VdbMetadataField> filterAttributesForModelReview(List<VdbMetadataField> metadataFields,
            SchemaInterpretation schemaInterpretation) {
        List<VdbMetadataField> filteredMetadataFields = new ArrayList<>();
        Table schemaTable = SchemaRepository.instance().getSchema(schemaInterpretation);

        for (VdbMetadataField metadataField : metadataFields) {
            if (metadataField.getTags() != null && metadataField.getTags().contains("Internal")) {
                if ((metadataField.getApprovedUsage() == null || metadataField.getApprovedUsage().equals("None"))
                        && (metadataField.getAssociatedRules().isEmpty()
                                || (schemaTable.getAttribute(metadataField.getColumnName()) != null
                                        && schemaTable.getAttribute(metadataField.getColumnName()).getApprovedUsage()
                                                .contains("None")))) {
                    continue;
                }
                filteredMetadataFields.add(metadataField);
            }
        }

        return filteredMetadataFields;
    }

}
