package com.latticeengines.pls.workflow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.dataflow.flows.DedupEventTableParameters;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.pls.CloneModelingParameters;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.leadprioritization.workflow.MatchAndModelWorkflowConfiguration;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchCommandProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component
public class MatchAndModelWorkflowSubmitter extends BaseModelWorkflowSubmitter {

    @Autowired
    private MatchCommandProxy matchCommandProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private SourceFileService sourceFileService;

    @Value("${pls.fitflow.stoplist.path}")
    private String stoplistPath;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ImportMatchAndModelWorkflowSubmitter.class);

    public ApplicationId submit(String cloneTableName, CloneModelingParameters parameters,
            List<Attribute> userRefinedAttributes, ModelSummary modelSummary) {
        TransformationGroup transformationGroup;
        String originalTransformationGroup = getTransformationGroupNameForModelSummary(modelSummary);
        if (parameters.enableTransformation() && originalTransformationGroup.equals("none")) {
            transformationGroup = getTransformationGroupFromZK();
        } else if (parameters.enableTransformation()) {
            transformationGroup = TransformationGroup.fromName(originalTransformationGroup);
        } else {
            transformationGroup = TransformationGroup.NONE;
        }

        MatchAndModelWorkflowConfiguration configuration = generateConfiguration(cloneTableName, parameters,
                transformationGroup, userRefinedAttributes, modelSummary);
        return workflowJobService.submit(configuration);
    }

    public MatchAndModelWorkflowConfiguration generateConfiguration(String cloneTableName,
            CloneModelingParameters parameters, TransformationGroup transformationGroup,
            List<Attribute> userRefinedAttributes, ModelSummary modelSummary) {
        String sourceSchemaInterpretation = modelSummary.getSourceSchemaInterpretation();
        MatchClientDocument matchClientDocument = matchCommandProxy.getBestMatchClient(3000);
        SourceFile sourceFile = sourceFileService.findByTableName(cloneTableName);

        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "modelAndEmailWorkflow");
        inputProperties.put(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME, parameters.getDisplayName());
        inputProperties.put(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME,
                sourceFile != null ? sourceFile.getDisplayName() : cloneTableName);

        Map<String, String> extraSources = new HashMap<>();
        extraSources.put("PublicDomain", stoplistPath);

        List<DataRule> dataRules = parameters.getDataRules();
        if (parameters.getDataRules() == null || parameters.getDataRules().isEmpty()) {
            Table eventTable = metadataProxy.getTable(MultiTenantContext.getCustomerSpace().toString(),
                    modelSummary.getEventTableName());
            dataRules = eventTable.getDataRules();
        }

        String trainingFilePath = modelSummary.getModelSummaryConfiguration()
                .getString(ProvenancePropertyName.TrainingFilePath, "");

        MatchAndModelWorkflowConfiguration.Builder builder = new MatchAndModelWorkflowConfiguration.Builder()
                .microServiceHostPort(microserviceHostPort) //
                .customer(getCustomerSpace()) //
                .workflow("modelAndEmailWorkflow") //
                .modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .modelName(parameters.getName()) //
                .displayName(parameters.getDisplayName()) //
                .internalResourceHostPort(internalResourceHostPort) //
                .sourceSchemaInterpretation(sourceSchemaInterpretation) //
                .inputProperties(inputProperties) //
                .trainingTableName(cloneTableName) //
                .transformationGroup(transformationGroup) //
                .enableV2Profiling(isV2ProfilingEnabled()) //
                .sourceModelSummary(modelSummary) //
                .dedupDataFlowBeanName("dedupEventTable") //
                .dedupDataFlowParams(new DedupEventTableParameters(cloneTableName, "PublicDomain",
                        parameters.getDeduplicationType())) //
                .dedupFlowExtraSources(extraSources) //
                .matchClientDocument(matchClientDocument) //
                .excludePublicDomains(parameters.isExcludePublicDomains()) //
                .skipDedupStep(parameters.getDeduplicationType() == DedupType.MULTIPLELEADSPERDOMAIN)
                .skipMatchingStep(parameters.isExcludePropDataAttributes()) //
                .skipStandardTransform(!parameters.enableTransformation()) //
                .addProvenanceProperty(ProvenancePropertyName.TrainingFilePath, trainingFilePath) //
                .matchType(MatchCommandType.MATCH_WITH_UNIVERSE) //
                .matchDestTables("DerivedColumnsCache") //
                .dataCloudVersion(getDataCloudVersion()).matchColumnSelection(Predefined.getDefaultSelection(), null)
                .moduleName(modelSummary.getModuleName()) //
                .pivotArtifactPath(modelSummary.getPivotArtifactPath()) //
                .isDefaultDataRules(false) //
                .dataRules(dataRules) //
                .userRefinedAttributes(userRefinedAttributes);
        if (parameters.getDeduplicationType() == DedupType.ONELEADPERDOMAIN) {
            builder.dedupTargetTableName(cloneTableName + "_deduped");
        } else if (parameters.getDeduplicationType() == DedupType.MULTIPLELEADSPERDOMAIN) {
            builder.dedupTargetTableName(cloneTableName);
        }
        return builder.build();
    }

    private String getDataCloudVersion() {
        if (useDnBFlagFromZK()) {
            // retrieve latest version from matchapi
            return columnMetadataProxy.latestVersion(null).getVersion();
        }
        return null;
    }

    private String getTransformationGroupNameForModelSummary(ModelSummary modelSummary) {
        String transformationGroupName = modelSummary.getModelSummaryConfiguration()
                .getString(ProvenancePropertyName.TransformationGroupName, null);
        if (transformationGroupName == null) {
            transformationGroupName = modelSummary.getTransformationGroupName();
        }
        if (transformationGroupName == null) {
            throw new LedpException(LedpCode.LEDP_18108, new String[] { modelSummary.getId() });
        }

        return transformationGroupName;
    }

}
