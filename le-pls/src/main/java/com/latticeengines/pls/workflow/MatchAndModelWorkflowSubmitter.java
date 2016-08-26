package com.latticeengines.pls.workflow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

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
import com.latticeengines.domain.exposed.propdata.MatchClientDocument;
import com.latticeengines.domain.exposed.propdata.MatchCommandType;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.leadprioritization.workflow.MatchAndModelWorkflowConfiguration;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.propdata.MatchCommandProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component
public class MatchAndModelWorkflowSubmitter extends BaseModelWorkflowSubmitter {

    @Autowired
    private MatchCommandProxy matchCommandProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    @Value("${pls.fitflow.stoplist.path}")
    private String stoplistPath;

    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ImportMatchAndModelWorkflowSubmitter.class);

    public ApplicationId submit(String cloneTableName, CloneModelingParameters parameters,
            List<Attribute> userRefinedAttributes, ModelSummary modelSummary) {
        String modelSummaryId = modelSummary.getId();

        String transformationGroupName = modelSummary.getTransformationGroupName();
        if (transformationGroupName == null) {
            throw new LedpException(LedpCode.LEDP_18108, new String[] { modelSummaryId });
        }

        MatchAndModelWorkflowConfiguration configuration = generateConfiguration(cloneTableName,
                parameters, TransformationGroup.fromName(transformationGroupName),
                userRefinedAttributes, modelSummary);
        return workflowJobService.submit(configuration);
    }

    public MatchAndModelWorkflowConfiguration generateConfiguration(String cloneTableName,
            CloneModelingParameters parameters, TransformationGroup transformationGroup,
            List<Attribute> userRefinedAttributes, ModelSummary modelSummary) {
        String sourceSchemaInterpretation = modelSummary.getSourceSchemaInterpretation();
        MatchClientDocument matchClientDocument = matchCommandProxy.getBestMatchClient(3000);

        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "modelAndEmailWorkflow");
        inputProperties.put(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME,
                parameters.getDisplayName());

        Map<String, String> extraSources = new HashMap<>();
        extraSources.put("PublicDomain", stoplistPath);

        List<DataRule> dataRules = parameters.getDataRules();
        if (parameters.getDataRules() == null || parameters.getDataRules().isEmpty()) {
            Table eventTable = metadataProxy.getTable(
                    MultiTenantContext.getCustomerSpace().toString(),
                    modelSummary.getEventTableName());
            dataRules = eventTable.getDataRules();
        }

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
                .sourceModelSummary(modelSummary) //
                .dedupDataFlowBeanName("dedupEventTable") //
                .dedupDataFlowParams(new DedupEventTableParameters(cloneTableName, "PublicDomain",
                        parameters.getDeduplicationType())) //
                .dedupFlowExtraSources(extraSources) //
                .matchClientDocument(matchClientDocument) //
                .excludePublicDomains(parameters.isExcludePublicDomains()) //
                .addProvenanceProperty(ProvenancePropertyName.ExcludePublicDomains,
                        parameters.isExcludePublicDomains()) //
                .addProvenanceProperty(ProvenancePropertyName.ExcludePropdataColumns,
                        parameters.isExcludePropDataAttributes()) //
                .addProvenanceProperty(ProvenancePropertyName.IsOneLeadPerDomain,
                        parameters.getDeduplicationType() == DedupType.ONELEADPERDOMAIN) //
                .matchType(MatchCommandType.MATCH_WITH_UNIVERSE) //
                .matchDestTables("DerivedColumnsCache") //
                .matchColumnSelection(Predefined.getDefaultSelection(), null)
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

}
