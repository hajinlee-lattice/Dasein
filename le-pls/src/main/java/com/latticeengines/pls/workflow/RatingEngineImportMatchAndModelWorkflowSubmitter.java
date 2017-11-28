package com.latticeengines.pls.workflow;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.modelreview.DataRuleListName;
import com.latticeengines.domain.exposed.modelreview.DataRuleLists;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.pls.RatingEngineModelingParameters;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.serviceflows.cdl.RatingEngineImportMatchAndModelWorkflowConfiguration;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchCommandProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component
public class RatingEngineImportMatchAndModelWorkflowSubmitter extends BaseModelWorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineImportMatchAndModelWorkflowSubmitter.class);

    @Autowired
    private MatchCommandProxy matchCommandProxy;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    public RatingEngineImportMatchAndModelWorkflowConfiguration generateConfiguration(
            RatingEngineModelingParameters parameters) {

        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "RatingEngineImportMatchAndModelWorkflow");
        inputProperties.put(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME, parameters.getDisplayName());

        MatchClientDocument matchClientDocument = matchCommandProxy.getBestMatchClient(3000);

        Predefined predefinedSelection = Predefined.getDefaultSelection();
        String predefinedSelectionName = parameters.getPredefinedSelectionName();
        if (StringUtils.isNotEmpty(predefinedSelectionName)) {
            predefinedSelection = Predefined.fromName(predefinedSelectionName);
            if (predefinedSelection == null) {
                throw new IllegalArgumentException("Cannot parse column selection named " + predefinedSelectionName);
            }
        }

        String moduleName = parameters.getModuleName();
        log.info("Modeling parameters: " + parameters.toString());

        String tableName = getTableName(parameters);
        RatingEngineImportMatchAndModelWorkflowConfiguration.Builder builder = new RatingEngineImportMatchAndModelWorkflowConfiguration.Builder()
                .microServiceHostPort(microserviceHostPort) //
                .customer(getCustomerSpace()) //
                .matchInputTableName(tableName) //
                .filterTableNames(parameters.getTrainFilterTableName(), parameters.getTargetFilterTableName()) //
                .filterQueries(parameters.getTrainFilterQuery(), parameters.getTargetFilterQuery()) //
                .internalResourceHostPort(internalResourceHostPort) //
                .userId(parameters.getUserId()) //
                .modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .excludeDataCloudAttrs(parameters.getExcludePropDataColumns()) //
                .skipDedupStep(parameters.getDeduplicationType() == DedupType.MULTIPLELEADSPERDOMAIN) //
                .matchRequestSource(MatchRequestSource.MODELING) //
                .matchQueue(LedpQueueAssigner.getModelingQueueNameForSubmission()) //
                .matchColumnSelection(predefinedSelection, parameters.getSelectedVersion()) //
                // null means latest
                .dataCloudVersion(getDataCloudVersion(parameters)) //
                .modelName(parameters.getName()) //
                .displayName(parameters.getDisplayName()) //
                .sourceSchemaInterpretation(SchemaInterpretation.SalesforceAccount.toString()) //
                .trainingTableName(tableName) //
                .inputProperties(inputProperties) //
                .enableV2Profiling(false) //
                .cdlModel(true) //
                .excludePublicDomains(parameters.isExcludePublicDomains()) //
                .addProvenanceProperty(ProvenancePropertyName.TrainingFilePath, getTrainPath(parameters)) //
                .addProvenanceProperty(ProvenancePropertyName.FuzzyMatchingEnabled,
                        plsFeatureFlagService.isFuzzyMatchEnabled()) //
                .moduleName(moduleName != null ? moduleName : null) //
                .isDefaultDataRules(true) //
                .dataRules(DataRuleLists.getDataRules(DataRuleListName.STANDARD)) //
                // TODO: legacy SQL based match engine configurations
                .matchClientDocument(matchClientDocument) //
                .matchType(MatchCommandType.MATCH_WITH_UNIVERSE) //
                .matchDestTables("DerivedColumnsCache") //
                .setRetainLatticeAccountId(true) //
                .setActivateModelSummaryByDefault(parameters.getActivateModelSummaryByDefault()) //
                .notesContent(parameters.getNotesContent());
        return builder.build();
    }

    private String getTableName(RatingEngineModelingParameters parameters) {
        String tableName = parameters.getTableName();
        if (StringUtils.isNotEmpty(tableName)) {
            return tableName;
        }
        return "RatingEngineModel_" + System.currentTimeMillis();
    }

    private String getTrainPath(RatingEngineModelingParameters parameters) {
        String outputFileName = "file_" + getTableName(parameters) + ".csv";
        String outputPath = PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(), getCustomerSpace()).toString()
                + "/" + outputFileName;
        return outputPath;
    }

    private String getDataCloudVersion(ModelingParameters parameters) {
        if (StringUtils.isNotEmpty(parameters.getDataCloudVersion())) {
            return parameters.getDataCloudVersion();
        }
        if (plsFeatureFlagService.useDnBFlagFromZK()) {
            return columnMetadataProxy.latestVersion(null).getVersion();
        }
        return null;
    }

    public ApplicationId submit(RatingEngineModelingParameters parameters) {
        TransformationGroup transformationGroup = plsFeatureFlagService.getTransformationGroupFromZK();
        if (parameters.getTransformationGroup() == null) {
            parameters.setTransformationGroup(transformationGroup);
        }
        RatingEngineImportMatchAndModelWorkflowConfiguration configuration = generateConfiguration(parameters);
        ApplicationId applicationId = workflowJobService.submit(configuration);
        return applicationId;
    }

}
