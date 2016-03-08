package com.latticeengines.serviceflows.workflow.modeling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.SemanticType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.core.ModelingServiceExecutor;

@Component("profileAndModel")
public class ProfileAndModel extends BaseWorkflowStep<ModelStepConfiguration> {

    private static final Log log = LogFactory.getLog(ProfileAndModel.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public void execute() {
        log.info("Inside ProfileAndModel execute()");

        Table eventTable = getEventTable();
        Map<String, String> modelApplicationIdToEventColumn;
        try {
            modelApplicationIdToEventColumn = profileAndModel(eventTable);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_28007, e, new String[] { eventTable.getName() });
        }

        executionContext.putString(MODEL_APP_IDS, JsonUtils.serialize(modelApplicationIdToEventColumn));
    }

    private Table getEventTable() {
        if (configuration.getEventTableName() != null) {
            return metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                    configuration.getEventTableName());
        } else {
            return JsonUtils.deserialize(executionContext.getString(EVENT_TABLE), Table.class);
        }
    }

    private Map<String, String> profileAndModel(Table eventTable) throws Exception {
        Map<String, String> modelApplicationIdToEventColumn = new HashMap<>();
        ModelingServiceExecutor.Builder bldr = createModelingServiceExecutorBuilder(configuration, eventTable);

        List<String> excludedColumns = new ArrayList<>();

        for (Attribute event : eventTable.getAttributes(SemanticType.Event)) {
            excludedColumns.add(event.getName());
        }

        for (Attribute attr : eventTable.getAttributes()) {
            if (attr.getApprovedUsage() == null //
                    || attr.getApprovedUsage().size() == 0 || attr.getApprovedUsage().get(0).equals("None")) {
                excludedColumns.add(attr.getName());
            }
        }

        String[] excludeList = new String[excludedColumns.size()];
        excludedColumns.toArray(excludeList);
        bldr = bldr.profileExcludeList(excludeList);

        for (Attribute event : eventTable.getAttributes(SemanticType.Event)) {
            bldr = bldr.targets(event.getName()) //
                    .metadataTable(String.format("%s-%s-Metadata", eventTable.getName(), event.getDisplayName())) //
                    .keyColumn("Id").modelName(configuration.getModelName()) //
                    .eventTableName(getEventTable().getName()) //
                    .sourceSchemaInterpretation(getConfiguration().getSourceSchemaInterpretation()) //
                    .productType(configuration.getProductType());
            if (eventTable.getAttributes(SemanticType.Event).size() > 1) {
                bldr = bldr.modelName(configuration.getModelName() + " (" + event.getDisplayName() + ")");
            }
            ModelingServiceExecutor modelExecutor = new ModelingServiceExecutor(bldr);
            modelExecutor.writeMetadataFiles();
            modelExecutor.profile();
            String modelAppId = modelExecutor.model();
            modelApplicationIdToEventColumn.put(modelAppId, event.getName());
        }
        return modelApplicationIdToEventColumn;
    }
}
