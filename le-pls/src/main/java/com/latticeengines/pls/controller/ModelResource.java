package com.latticeengines.pls.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.util.NameValidationUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modelreview.ColumnRuleResult;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.modelreview.ModelReviewData;
import com.latticeengines.domain.exposed.modelreview.RowRuleResult;
import com.latticeengines.domain.exposed.pls.CloneModelingParameters;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.pls.service.ModelCopyService;
import com.latticeengines.pls.service.ModelMetadataService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.pls.workflow.ImportMatchAndModelWorkflowSubmitter;
import com.latticeengines.pls.workflow.MatchAndModelWorkflowSubmitter;
import com.latticeengines.pls.workflow.PMMLModelWorkflowSubmitter;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

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
    private MetadataProxy metadataProxy;

    @Value("${pls.microservice.rest.endpoint.hostport}")
    private String microserviceEndpoint;

    @RequestMapping(value = "/{modelName}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Generate a model from the supplied file and parameters. Returns the job id.")
    public ResponseDocument<String> model(@PathVariable String modelName, //
            @RequestBody ModelingParameters parameters) {
        if (!NameValidationUtils.validateModelName(modelName)) {
            String message = String.format("Not qualified modelName %s contains unsupported characters.", modelName);
            log.error(message);
            throw new RuntimeException(message);
        }
        log.info(String.format("model called with parameters %s", parameters.toString()));
        return ResponseDocument.successResponse( //
                importMatchAndModelWorkflowSubmitter.submit(parameters).toString());

    }

    @RequestMapping(value = "/{modelName}/clone", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Clones and remodels with the specified model name.")
    public ResponseDocument<String> cloneAndRemodel(@PathVariable String modelName,
            @RequestBody CloneModelingParameters parameters) {
        log.info(String.format("cloneAndRemodel called with parameters %s, dedupOption: %s", parameters.toString(),
                parameters.getDeduplicationType()));
        Table clone = modelMetadataService.cloneTrainingTable(parameters.getSourceModelSummaryId());
        List<Attribute> userRefinedAttributes = modelMetadataService.getAttributesFromFields(clone.getAttributes(),
                parameters.getAttributes());
        ModelSummary modelSummary = modelSummaryService.getModelSummaryEnrichedByDetails(parameters
                .getSourceModelSummaryId());
        return ResponseDocument.successResponse( //
                modelWorkflowSubmitter.submit(clone.getName(), parameters, userRefinedAttributes, modelSummary)
                        .toString());
    }

    @RequestMapping(value = "/pmml/{modelName}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Generate a PMML model from the supplied module. Returns the job id.")
    public ResponseDocument<String> modelForPmml(@PathVariable String modelName, //
            @RequestParam(value = "module") String moduleName, //
            @RequestParam(value = "pivotfile") String pivotFileName, //
            @RequestParam(value = "pmmlfile") String pmmlFileName) {
        if (!NameValidationUtils.validateModelName(modelName)) {
            String message = String.format("Not qualified modelName %s contains unsupported characters.", modelName);
            log.error(message);
            throw new RuntimeException(message);
        }
        String appId = pmmlModelWorkflowSubmitter.submit(modelName, moduleName, pivotFileName, pmmlFileName).toString();
        return ResponseDocument.successResponse(appId);

    }

    @RequestMapping(value = "/copymodel/{modelName}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Copy a model from current tenant to target tenant.")
    public ResponseDocument<Boolean> copyModel(@PathVariable String modelName,
            @RequestParam(value = "targetTenantId") String targetTenantId) {
        return ResponseDocument.successResponse( //
                modelCopyService.copyModel(targetTenantId, modelName));
    }

    @RequestMapping(value = "/reviewmodel/{modelName}/{eventTableName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the model review data rules for the model")
    public ResponseDocument<ModelReviewData> getModelReviewData(@PathVariable String modelName, @PathVariable String eventTableName)
            throws IOException{
        return ResponseDocument.successResponse(metadataProxy.getReviewData(modelName, eventTableName));
    }

    @RequestMapping(value = "/reviewmodel/mocked/{modelName}/{eventTableName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the model review data rules for the model")
    public ResponseDocument<ModelReviewData> getMockedModelReviewData(@PathVariable String modelName, @PathVariable String eventTableName)
            throws IOException{
        return ResponseDocument.successResponse(generateStubData());
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

    @SuppressWarnings("unused")
    private ModelReviewData generateStubData() {
        Triple<List<DataRule>, Map<String, ColumnRuleResult>, Map<String, RowRuleResult>> masterList = getMasterList();

        ModelReviewData reviewData = new ModelReviewData();
        reviewData.setDataRules(masterList.getLeft());
        reviewData.setRuleNameToColumnRuleResults(masterList.getMiddle());
        reviewData.setRuleNameToRowRuleResults(masterList.getRight());

        return reviewData;
    }

    @SuppressWarnings("unchecked")
    private Triple<List<DataRule>, Map<String, ColumnRuleResult>, Map<String, RowRuleResult>> getMasterList() {
        List<Triple<String, String, Boolean>> masterColumnConfig = new ArrayList<>();
        List<Triple<String, String, Boolean>> masterRowConfig = new ArrayList<>();

        Triple<String, String, Boolean> overlyPredictiveColumns = Triple.of("Overly Predictive Columns",
                "overly predictive single category / value range", false);
        masterColumnConfig.add(overlyPredictiveColumns);
        Triple<String, String, Boolean> lowCoverage = Triple
                .of("Low Coverage", "Low coverage (empty exceeds x%)", false);
        masterColumnConfig.add(lowCoverage);
        Triple<String, String, Boolean> populatedRowCount = Triple.of("Populated Row Count",
                "Populated Row Count - Integrated from Profiling (certain value exceeds x%) ", false);
        masterColumnConfig.add(populatedRowCount);
        Triple<String, String, Boolean> positivelyPredictiveNulls = Triple.of("Positively Predictive Nulls",
                "Positively predictive nulls", false);
        masterColumnConfig.add(positivelyPredictiveNulls);
        Triple<String, String, Boolean> uniqueValueCount = Triple.of("Unique Value Count",
                "Unique value count in column - Integrated from Profiling", false);
        masterColumnConfig.add(uniqueValueCount);
        Triple<String, String, Boolean> publicDomains = Triple.of("Public Domains",
                "Exclude Records with Public Domains ", false);
        masterRowConfig.add(publicDomains);
        Triple<String, String, Boolean> customDomains = Triple.of("Custom Domains", "Exclude specific domain(s)", false);
        masterRowConfig.add(customDomains);
        Triple<String, String, Boolean> oneRecordPerDomain = Triple.of("One Record Per Domain", "One Record Per Domain",
                false);
        masterRowConfig.add(oneRecordPerDomain);
        Triple<String, String, Boolean> oneLeadPerAccount = Triple.of("One Lead Per Account", "One Lead Per Account",
                false);
        masterRowConfig.add(oneLeadPerAccount);
        Triple<String, String, Boolean> highPredictiveLowPopulation = Triple.of("High Predictive Low Population",
                "High predictive, low population", false);
        masterRowConfig.add(highPredictiveLowPopulation);

        List<DataRule> masterRuleList = new ArrayList<>();
        Map<String, ColumnRuleResult> columnResults = new HashMap<>();
        for (Triple<String, String, Boolean> config : masterColumnConfig) {
            DataRule rule = generateDataRule(config);
            masterRuleList.add(rule);
            ColumnRuleResult columnResult = new ColumnRuleResult();
            columnResult.setDataRuleName(rule.getName());
            if (rule.getName().equals("UniqueValueCount")) {
                List<String> flaggedColumns = new ArrayList<>();
                flaggedColumns.add("SomeColumnA");
                flaggedColumns.add("AnotherColumnB");
                columnResult.setFlaggedColumnNames(flaggedColumns);
            } else {
                columnResult.setFlaggedColumnNames(Collections.EMPTY_LIST);
            }
            columnResult.setFlaggedItemCount(columnResult.getFlaggedColumnNames().size());
            columnResults.put(rule.getName(), columnResult);
        }

        Map<String, RowRuleResult> rowResults = new HashMap<>();
        for (Triple<String, String, Boolean> config : masterRowConfig) {
            DataRule rule = generateDataRule(config);
            masterRuleList.add(rule);
            RowRuleResult rowResult = new RowRuleResult();
            rowResult.setDataRuleName(rule.getName());
            rowResult.setFlaggedItemCount(0);
            rowResult.setFlaggedRowIdAndColumnNames(Collections.EMPTY_MAP);
            rowResult.setNumPositiveEvents(0);
            rowResults.put(rule.getName(), rowResult);
        }

        return Triple.of(masterRuleList, columnResults, rowResults);
    }

    @SuppressWarnings("unchecked")
    private DataRule generateDataRule(Triple<String, String, Boolean> config) {
        DataRule rule = new DataRule();
        rule.setName(StringUtils.trimAllWhitespace(config.getLeft()));
        rule.setDisplayName(config.getLeft());
        rule.setDescription(config.getMiddle());
        rule.setFrozenEnablement(config.getRight());
        rule.setColumnsToRemediate(Collections.EMPTY_LIST);
        if (rule.getName().equals("CustomDomains")) {
            Map<String, String> props = new HashMap<>();
            props.put("domains", "company.com, anothersite.com, abc.com");
            rule.setProperties(props);
        } else {
            rule.setProperties(Collections.EMPTY_MAP);
        }
        return rule;
    }
}
