package com.latticeengines.pls.workflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataflow.flows.DedupEventTableParameters;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.PivotValuesLookup;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.propdata.MatchClientDocument;
import com.latticeengines.domain.exposed.propdata.MatchCommandType;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.util.DataRuleUtils;
import com.latticeengines.domain.exposed.util.ModelingUtils;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.leadprioritization.workflow.ImportMatchAndModelWorkflowConfiguration;
import com.latticeengines.pls.service.MetadataFileUploadService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.util.PivotMappingFileUtils;
import com.latticeengines.proxy.exposed.propdata.MatchCommandProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component
public class ImportMatchAndModelWorkflowSubmitter extends BaseModelWorkflowSubmitter {

    private static final Logger log = Logger.getLogger(ImportMatchAndModelWorkflowSubmitter.class);

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private MatchCommandProxy matchCommandProxy;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private MetadataFileUploadService metadataFileUploadService;

    @Value("${pls.modeling.validation.min.dedupedrows:300}")
    private long minDedupedRows;

    @Value("${pls.modeling.validation.min.eventrows:50}")
    private long minPositiveEvents;

    @Value("${pls.fitflow.stoplist.path}")
    private String stoplistPath;

    public ImportMatchAndModelWorkflowConfiguration generateConfiguration(ModelingParameters parameters) {

        SourceFile sourceFile = sourceFileService.findByName(parameters.getFilename());

        if (sourceFile == null) {
            throw new LedpException(LedpCode.LEDP_18084, new String[] { parameters.getFilename() });
        }

        String trainingTableName = sourceFile.getTableName();

        if (trainingTableName == null) {
            throw new LedpException(LedpCode.LEDP_18099, new String[] { sourceFile.getDisplayName() });
        }

        if (hasRunningWorkflow(sourceFile)) {
            throw new LedpException(LedpCode.LEDP_18081, new String[] { sourceFile.getDisplayName() });
        }

        MatchClientDocument matchClientDocument = matchCommandProxy.getBestMatchClient(3000);

        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "importMatchAndModelWorkflow");

        Map<String, String> extraSources = new HashMap<>();
        extraSources.put("PublicDomain", stoplistPath);

        ColumnSelection.Predefined predefinedSelection = ColumnSelection.Predefined.getDefaultSelection();
        String predefinedSelectionName = parameters.getPredefinedSelectionName();
        if (StringUtils.isNotEmpty(predefinedSelectionName)) {
            predefinedSelection = ColumnSelection.Predefined.fromName(predefinedSelectionName);
            if (predefinedSelection == null) {
                throw new IllegalArgumentException("Cannot parse column selection named " + predefinedSelectionName);
            }
        }

        String moduleName = parameters.getModuleName();
        final String pivotFileName = parameters.getPivotFileName();
        Artifact pivotArtifact = null;

        if (StringUtils.isNotEmpty(moduleName) && StringUtils.isNotEmpty(pivotFileName)) {
            List<Artifact> pivotArtifacts = metadataFileUploadService.getArtifacts(moduleName,
                    ArtifactType.PivotMapping);
            pivotArtifact = Iterables.find(pivotArtifacts, new Predicate<Artifact>() {
                @Override
                public boolean apply(Artifact artifact) {
                    return artifact.getName().equals(pivotFileName);
                }
            }, null);
            if (pivotFileName != null && pivotArtifact == null) {
                throw new LedpException(LedpCode.LEDP_28026, new String[] { pivotFileName, moduleName });
            }
        }
        if (pivotArtifact != null) {
            updateTrainingTable(pivotArtifact.getPath(), trainingTableName);
        }

        log.info("Modeling parameters: " + parameters.toString());
        ImportMatchAndModelWorkflowConfiguration configuration = new ImportMatchAndModelWorkflowConfiguration.Builder()
                .microServiceHostPort(microserviceHostPort) //
                .customer(getCustomerSpace()) //
                .sourceFileName(sourceFile.getName()) //
                .sourceType(SourceType.FILE) //
                .internalResourceHostPort(internalResourceHostPort) //
                .importReportNamePrefix(sourceFile.getName() + "_Report") //
                .eventTableReportNamePrefix(sourceFile.getName() + "_EventTableReport") //
                .dedupDataFlowBeanName("dedupEventTable")
                .dedupDataFlowParams( //
                        new DedupEventTableParameters(sourceFile.getTableName(), "PublicDomain", parameters
                                .getDeduplicationType())) //
                .dedupFlowExtraSources(extraSources) //
                .dedupTargetTableName(sourceFile.getTableName() + "_deduped") //
                .modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .matchClientDocument(matchClientDocument) //
                .excludePublicDomains(parameters.isExcludePublicDomains()) //
                .matchType(MatchCommandType.MATCH_WITH_UNIVERSE) //
                .matchDestTables("DerivedColumnsCache") //
                .matchColumnSelection(predefinedSelection, parameters.getSelectedVersion()) // null
                                                                                            // means
                                                                                            // latest
                .modelName(parameters.getName()) //
                .displayName(parameters.getDisplayName()) //
                .sourceSchemaInterpretation(sourceFile.getSchemaInterpretation().toString()) //
                .trainingTableName(trainingTableName) //
                .inputProperties(inputProperties) //
                .minDedupedRows(minDedupedRows) //
                .minPositiveEvents(minPositiveEvents) //
                .transformationGroup(parameters.getTransformationGroup()) //
                .excludePropDataColumns(parameters.getExcludePropDataColumns()) //
                .pivotArtifactPath(pivotArtifact != null ? pivotArtifact.getPath() : null) //
                .runTimeParams(parameters.runTimeParams) //
                .isDefaultDataRules(true) //
                .dataRules(createDefaultDataRules(sourceFile.getSchemaInterpretation())) //
                .build();
        return configuration;
    }

    public ApplicationId submit(ModelingParameters parameters) {
        SourceFile sourceFile = sourceFileService.findByName(parameters.getFilename());

        TransformationGroup transformationGroup = getTransformationGroupFromZK();

        if (parameters.getTransformationGroup() == null) {
            parameters.setTransformationGroup(transformationGroup);
        }

        ImportMatchAndModelWorkflowConfiguration configuration = generateConfiguration(parameters);

        ApplicationId applicationId = workflowJobService.submit(configuration);
        sourceFile.setApplicationId(applicationId.toString());
        sourceFileService.update(sourceFile);
        return applicationId;
    }

    private void updateTrainingTable(String pivotArtifactPath, String trainingTableName) {
        Table trainingTable = metadataProxy.getTable(MultiTenantContext.getCustomerSpace().toString(),
                trainingTableName);
        List<Attribute> trainingAttrs = trainingTable.getAttributes();
        PivotValuesLookup pivotValues = null;
        try {
            pivotValues = ModelingUtils.getPivotValues(yarnConfiguration, pivotArtifactPath);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
        if (pivotValues == null) {
            throw new RuntimeException("PivotValuesLookup is null.");
        }
        Set<String> sourceColumnNames = pivotValues.pivotValuesBySourceColumn.keySet();
        List<Attribute> attrs = PivotMappingFileUtils.createAttrsFromPivotSourceColumns(sourceColumnNames,
                trainingAttrs);

        trainingTable.setAttributes(attrs);
        metadataProxy.updateTable(MultiTenantContext.getCustomerSpace().toString(), trainingTableName, trainingTable);
    }

    private List<DataRule> createDefaultDataRules(SchemaInterpretation schemaInterpretation) {
        List<DataRule> defaultRules = getMasterList();

        for (DataRule dataRule : defaultRules) {
            if (dataRule.getName().equals("UniqueValueCountDS")) {
                dataRule.setEnabled(true);
                Map<String, String> countUniqueValueRuleProps = new HashMap<>();
                countUniqueValueRuleProps.put("uniquevaluecountThreshold", String.valueOf(200));
                dataRule.setProperties(countUniqueValueRuleProps);
            } else if (dataRule.getName().equals("PopulatedRowCountDS")) {
                dataRule.setEnabled(true);
            }
        }

        log.info("Default rules submitted: " + JsonUtils.serialize(defaultRules));

        return defaultRules;
    }

    private List<DataRule> getMasterList() {
        List<String> columnRuleNames = new ArrayList<>();
        List<String> rowRuleNames = new ArrayList<>();

        columnRuleNames.add("UniqueValueCountDS");
        columnRuleNames.add("PopulatedRowCountDS");
        columnRuleNames.add("OverlyPredictiveDS");
        columnRuleNames.add("LowCoverageDS");
        columnRuleNames.add("NullIssueDS");

        rowRuleNames.add("HighlyPredictiveSmallPopulationDS");

        List<DataRule> masterRuleList = new ArrayList<>();
        for (String name : columnRuleNames) {
            DataRule rule = generateDataRule(name);
            masterRuleList.add(rule);
        }

        for (String name : rowRuleNames) {
            DataRule rule = generateDataRule(name);
            masterRuleList.add(rule);
        }

        DataRuleUtils.populateDataRuleDisplayNameAndDescriptions(masterRuleList);

        return masterRuleList;
    }

    private DataRule generateDataRule(String name) {
        DataRule rule = new DataRule();
        rule.setName(name);
        rule.setFrozenEnablement(false);
        rule.setColumnsToRemediate(new ArrayList<String>());
        rule.setEnabled(false);
        rule.setProperties(new HashMap<String, String>());

        return rule;
    }

}
