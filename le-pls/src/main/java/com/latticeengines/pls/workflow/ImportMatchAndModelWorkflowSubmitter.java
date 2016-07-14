package com.latticeengines.pls.workflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
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
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.propdata.MatchClientDocument;
import com.latticeengines.domain.exposed.propdata.MatchCommandType;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.leadprioritization.workflow.ImportMatchAndModelWorkflowConfiguration;
import com.latticeengines.pls.service.MetadataFileUploadService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.proxy.exposed.propdata.MatchCommandProxy;

@Component
public class ImportMatchAndModelWorkflowSubmitter extends BaseModelWorkflowSubmitter {

    private static final Logger log = Logger.getLogger(ImportMatchAndModelWorkflowSubmitter.class);

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private MatchCommandProxy matchCommandProxy;

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
            });
            if (pivotFileName != null && pivotArtifact == null) {
                throw new LedpException(LedpCode.LEDP_28026, new String[] { pivotFileName, moduleName });
            }
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
                //
                .dedupDataFlowParams(
                        new DedupEventTableParameters(sourceFile.getTableName(), "PublicDomain", parameters
                                .getDeduplicationType())) //
                .dedupFlowExtraSources(extraSources) //
                .dedupTargetTableName(sourceFile.getTableName() + "_deduped") //
                .modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .matchClientDocument(matchClientDocument) //
                .matchType(MatchCommandType.MATCH_WITH_UNIVERSE) //
                .matchDestTables("DerivedColumnsCache") //
                .matchColumnSelection(predefinedSelection, parameters.getSelectedVersion()) // null means
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

    private List<DataRule> createDefaultDataRules(SchemaInterpretation schemaInterpretation) {
        List<DataRule> defaultRules = getMasterList();

        for (DataRule dataRule : defaultRules) {
            if (dataRule.getName().equals("CountUniqueValueRule")) {
                dataRule.setEnabled(true);
                Map<String, String> countUniqueValueRuleProps = new HashMap<>();
                countUniqueValueRuleProps.put("uniqueCountThreshold", String.valueOf(200));
                dataRule.setProperties(countUniqueValueRuleProps);
            } else if (dataRule.getName().equals("PopulatedRowCount")) {
                dataRule.setEnabled(true);
            } else if (dataRule.getName().equals("OneRecordPerDomain")
                    && schemaInterpretation.equals(SchemaInterpretation.SalesforceAccount)) {
                dataRule.setEnabled(true);
            }
        }

        log.info("Default rules submitted: " + JsonUtils.serialize(defaultRules));

        return defaultRules;
    }

    private List<DataRule> getMasterList() {
        List<Triple<String, String, String>> masterColumnConfig = new ArrayList<>();
        List<Triple<String, String, String>> masterRowConfig = new ArrayList<>();

        Triple<String, String, String> uniqueValueCount = Triple.of("UniqueValueCountDS", "Count Unique Value Rule",
                "Unique value count in column - Integrated from Profiling");
        masterColumnConfig.add(uniqueValueCount);

        Triple<String, String, String> populatedRowCount = Triple.of("PopulatedRowCountDS", "Populated Row Count",
                "Populated Row Count - Integrated from Profiling (certain value exceeds x%) ");
        masterColumnConfig.add(populatedRowCount);

        Triple<String, String, String> overlyPredictiveColumns = Triple.of("OverlyPredictiveDS",
                "Overly Predictive Columns", "overly predictive single category / value range");
        masterColumnConfig.add(overlyPredictiveColumns);

        Triple<String, String, String> lowCoverage = Triple.of("LowCoverageDS", "Low Coverage",
                "Low coverage (empty exceeds x%)");
        masterColumnConfig.add(lowCoverage);

        Triple<String, String, String> positivelyPredictiveNulls = Triple.of("NullIssueDS",
                "Positively Predictive Nulls", "Positively predictive nulls");
        masterColumnConfig.add(positivelyPredictiveNulls);

        Triple<String, String, String> highPredictiveLowPopulation = Triple.of("HighlyPredictiveSmallPopulationDS",
                "High Predictive Low Population", "High predictive, low population");
        masterRowConfig.add(highPredictiveLowPopulation);

        List<DataRule> masterRuleList = new ArrayList<>();
        for (Triple<String, String, String> config : masterColumnConfig) {
            DataRule rule = generateDataRule(config);
            masterRuleList.add(rule);
        }

        for (Triple<String, String, String> config : masterRowConfig) {
            DataRule rule = generateDataRule(config);
            masterRuleList.add(rule);
        }

        return masterRuleList;
    }

    private DataRule generateDataRule(Triple<String, String, String> config) {
        DataRule rule = new DataRule();
        rule.setName(config.getLeft());
        rule.setDisplayName(config.getMiddle());
        rule.setDescription(config.getRight());
        rule.setFrozenEnablement(false);
        rule.setColumnsToRemediate(new ArrayList<String>());
        rule.setEnabled(false);
        rule.setProperties(new HashMap<String, String>());

        return rule;
    }

}
