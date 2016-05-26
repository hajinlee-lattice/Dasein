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
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.propdata.MatchClientDocument;
import com.latticeengines.domain.exposed.propdata.MatchCommandType;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.leadprioritization.workflow.MatchAndModelWorkflowConfiguration;
import com.latticeengines.proxy.exposed.propdata.MatchCommandProxy;

@Component
public class MatchAndModelWorkflowSubmitter extends BaseModelWorkflowSubmitter {

    @Autowired
    private MatchCommandProxy matchCommandProxy;

    @Value("${pls.fitflow.stoplist.path}")
    private String stoplistPath;

    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ImportMatchAndModelWorkflowSubmitter.class);

    public ApplicationId submit(String cloneTableName, String modelName, String displayName,
            List<Attribute> userRefinedAttributes, ModelSummary modelSummary) {
        String modelSummaryId = modelSummary.getId();

        String transformationGroupName = modelSummary.getTransformationGroupName();
        if (transformationGroupName == null) {
            throw new LedpException(LedpCode.LEDP_18108, new String[] { modelSummaryId });
        }

        MatchAndModelWorkflowConfiguration configuration = generateConfiguration(cloneTableName, modelName,
                displayName, TransformationGroup.fromName(transformationGroupName), userRefinedAttributes, modelSummary);
        return workflowJobService.submit(configuration);
    }

    public MatchAndModelWorkflowConfiguration generateConfiguration(String cloneTableName, String modelName,
            String displayName, TransformationGroup transformationGroup, List<Attribute> userRefinedAttributes,
            ModelSummary modelSummary) {
        String sourceSchemaInterpretation = modelSummary.getSourceSchemaInterpretation();
        MatchClientDocument matchClientDocument = matchCommandProxy.getBestMatchClient(3000);

        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "modelAndEmailWorkflow");

        Map<String, String> extraSources = new HashMap<>();
        extraSources.put("PublicDomain", stoplistPath);

        MatchAndModelWorkflowConfiguration configuration = new MatchAndModelWorkflowConfiguration.Builder()
                .microServiceHostPort(microserviceHostPort) //
                .customer(getCustomerSpace()) //
                .workflow("modelAndEmailWorkflow") //
                .modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .modelName(modelName) //
                .displayName(displayName) //
                .internalResourceHostPort(internalResourceHostPort) //
                .sourceSchemaInterpretation(sourceSchemaInterpretation) //
                .inputProperties(inputProperties) //
                .trainingTableName(cloneTableName) //
                .transformationGroupName(transformationGroup) //
                .sourceModelSummary(modelSummary) //
                .dedupDataFlowBeanName("dedupEventTable") //
                .dedupDataFlowParams(new DedupEventTableParameters(cloneTableName, "PublicDomain")) //
                .dedupFlowExtraSources(extraSources) //
                .dedupTargetTableName(cloneTableName + "_deduped") //
                .matchClientDocument(matchClientDocument) //
                .matchType(MatchCommandType.MATCH_WITH_UNIVERSE) //
                .matchDestTables("DerivedColumnsCache") //
                .matchColumnSelection(ColumnSelection.Predefined.getDefaultSelection(), null) // null
                                                                                              // means
                                                                                              // latest
                .userRefinedAttributes(userRefinedAttributes) //
                .build();
        return configuration;
    }

}
