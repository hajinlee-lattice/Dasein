package com.latticeengines.serviceflows.workflow.modeling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
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

        List<Attribute> events = eventTable.getAttributes(LogicalDataType.Event);
        if (events.isEmpty()) {
            throw new RuntimeException("No events in event table");
        }
        for (Attribute event : events) {
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

        for (Attribute event : events) {
            bldr = bldr.targets(getTargets(eventTable, event)) //
                    .metadataTable(String.format("%s-%s-Metadata", eventTable.getName(), event.getDisplayName())) //
                    .keyColumn("Id").modelName(configuration.getModelName()) //
                    .eventTableName(getEventTable().getName()) //
                    .sourceSchemaInterpretation(getConfiguration().getSourceSchemaInterpretation()) //
                    .productType(configuration.getProductType());
            if (events.size() > 1) {
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

    private String[] getTargets(Table eventTable, Attribute event) {
        List<String> targets = new ArrayList<>();

        targets.add("Event: " + event.getName());

        Attribute companyName = eventTable.getAttribute(InterfaceName.CompanyName);
        if (companyName != null) {
            targets.add("Company: " + companyName.getName());
        } else {
            log.info("No company attribute in this event table.");
        }

        Attribute lastName = eventTable.getAttribute(InterfaceName.LastName);
        if (lastName != null) {
            targets.add("LastName: " + lastName.getName());
        } else {
            log.info("No last name attribute in this event table.");
        }
        Attribute firstName = eventTable.getAttribute(InterfaceName.FirstName);
        if (firstName != null) {
            targets.add("FirstName: " + firstName.getName());
        } else {
            log.info("No company attribute in this event table.");
        }

        Attribute id = eventTable.getAttribute(InterfaceName.Id);
        Attribute email = eventTable.getAttribute(InterfaceName.Email);
        Attribute creationDate = eventTable.getAttribute(InterfaceName.CreatedDate);

        List<String> readoutAttributes = new ArrayList<>();
        if (id != null) {
            readoutAttributes.add(id.getName());
        }
        if (email != null) {
            readoutAttributes.add(email.getName());
        }
        if (creationDate != null) {
            readoutAttributes.add(creationDate.getName());
        }

        if (readoutAttributes.size() > 0) {
            targets.add("Readouts: " + StringUtils.join(readoutAttributes, "|"));
        } else {
            log.info("No id, email or creation date attribute in this event table.");
        }

        return targets.toArray(new String[targets.size()]);
    }
}
