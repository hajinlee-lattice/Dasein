package com.latticeengines.pls.workflow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.leadprioritization.workflow.ModelWorkflowConfiguration;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component
public class ModelWorkflowSubmitter extends BaseModelWorkflowSubmitter {
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ImportMatchAndModelWorkflowSubmitter.class);

    public ApplicationId submit(String eventTableName, String modelName, String displayName, ModelSummary summaryWithDetails) {
        String modelSummaryId = summaryWithDetails.getId();
        if (StringUtils.isEmpty(modelSummaryId)) {
            modelSummaryId = "unkown";
        }

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode details = objectMapper.readTree(summaryWithDetails.getDetails().getPayload());
            JsonNode provenance = details.get("EventTableProvenance");
            if (provenance != null) {
                String predefinedSelectionName = provenance.get("Predefined_ColumnSelection_Name").asText();
                if (org.apache.commons.lang.StringUtils.isNotEmpty(predefinedSelectionName)) {
                    ColumnSelection.Predefined predefined = ColumnSelection.Predefined
                            .fromName(predefinedSelectionName);
                    String predefinedSelectionVersion = provenance.get("Predefined_ColumnSelection_Version")
                            .asText();
                    summaryWithDetails.setPredefinedSelection(predefined);
                    summaryWithDetails.setPredefinedSelectionVersion(predefinedSelectionVersion);
                } else {
                    ColumnSelection selection = objectMapper
                            .treeToValue(provenance.get("Customized_ColumnSelection"), ColumnSelection.class);
                    summaryWithDetails.setCustomizedColumnSelection(selection);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        summaryWithDetails.setDetails(null);
        summaryWithDetails.setPredictors(new ArrayList<Predictor>());

        String sourceSchemaInterpretation = summaryWithDetails.getSourceSchemaInterpretation();
        String trainingTableName = summaryWithDetails.getTrainingTableName();
        String transformationGroupName = summaryWithDetails.getTransformationGroupName();
        if (transformationGroupName == null) {
            throw new LedpException(LedpCode.LEDP_18108, new String[]{modelSummaryId});
        }

        Table eventTable = metadataProxy.getTable(MultiTenantContext.getCustomerSpace().toString(), eventTableName);

        if (eventTable == null) {
            throw new LedpException(LedpCode.LEDP_18088, new String[]{eventTableName});
        }

        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "modelAndEmailWorkflow");

        ModelWorkflowConfiguration configuration = new ModelWorkflowConfiguration.Builder()
                .microServiceHostPort(microserviceHostPort) //
                .customer(getCustomerSpace()) //
                .workflow("modelWorkflow") //
                .modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .modelName(modelName) //
                .displayName(displayName) //
                .eventTableName(eventTableName) //
                .internalResourceHostPort(internalResourceHostPort) //
                .sourceSchemaInterpretation(sourceSchemaInterpretation) //
                .inputProperties(inputProperties) //
                .trainingTableName(trainingTableName) //
                .transformationGroupName(transformationGroupName) //
                .sourceModelSummary(summaryWithDetails) //
                .build();
        return workflowJobService.submit(configuration);
    }

}
