package com.latticeengines.serviceflows.workflow.modeling;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.core.ModelingServiceExecutor;

public abstract class BaseModelStep<T extends ModelStepConfiguration> extends BaseWorkflowStep<T> {

    private static final Log log = LogFactory.getLog(BaseModelStep.class);

    @Autowired
    protected MetadataProxy metadataProxy;

    protected Table getEventTable() {
        if (configuration.getEventTableName() != null) {
            return metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                    configuration.getEventTableName());
        } else {
            return JsonUtils.deserialize(executionContext.getString(EVENT_TABLE), Table.class);
        }
    }

    protected ColumnSelection.Predefined getPredefinedSelection() {
        return (ColumnSelection.Predefined) executionContext.get(MATCH_PREDEFINED_SELECTION);
    }

    protected String getPredefinedSelectionVersion() {
        return executionContext.getString(MATCH_PREDEFINED_SELECTION_VERSION);
    }

    protected ColumnSelection getCustomizedSelection() {
        return (ColumnSelection) executionContext.get(MATCH_CUSTOMIZED_SELECTION);
    }

    protected String getTransformationGroupName() {
        if (configuration.getEventTableName() != null) {
            return configuration.getTransformationGroupName();
        } else {
            return getStringValueFromContext(TRANSFORMATION_GROUP_NAME);
        }
    }

    protected String[] getTargets(Table eventTable, Attribute event) {
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

    protected ModelingServiceExecutor createModelingServiceExecutor(Table eventTable, Attribute currentEvent)
            throws Exception {
        ModelingServiceExecutor.Builder bldr = createModelingServiceExecutorBuilder(configuration, eventTable);

        List<String> excludedColumns = new ArrayList<>();

        List<Attribute> events = eventTable.getAttributes(LogicalDataType.Event);
        if (events.isEmpty()) {
            throw new RuntimeException("No events in event table");
        }
        for (Attribute event : events) {
            excludedColumns.add(event.getName());
        }

        log.info("Exclude prop data columns = " + configuration.excludePropDataColumns());

        for (Attribute attr : eventTable.getAttributes()) {
            if (attr.getApprovedUsage() == null //
                    || attr.getApprovedUsage().size() == 0 //
                    || attr.getApprovedUsage().get(0).equals("None")) {
                excludedColumns.add(attr.getName());
            } else if (configuration.excludePropDataColumns() && attr.getTags() != null) {
                Set<String> tags = new HashSet<>(attr.getTags());

                if (tags.contains(Tag.EXTERNAL.getName()) || tags.contains(Tag.EXTERNAL_TRANSFORM.getName())) {
                    excludedColumns.add(attr.getName());
                }
            }
        }

        String[] excludeList = new String[excludedColumns.size()];
        excludedColumns.toArray(excludeList);
        bldr = bldr.profileExcludeList(excludeList);

        bldr = bldr.targets(getTargets(eventTable, currentEvent)) //
                .metadataTable(getMetadataTableFolderName(eventTable, currentEvent)) //
                .keyColumn("Id").modelName(configuration.getModelName()) //
                .eventTableName(getEventTable().getName()) //
                .sourceSchemaInterpretation(getConfiguration().getSourceSchemaInterpretation()) //
                .trainingTableName(getConfiguration().getTrainingTableName()) //
                .transformationGroupName(getTransformationGroupName()) //
                .pivotArtifactPath(configuration.getPivotArtifactPath()) //
                .productType(configuration.getProductType()) //
                .runTimeParams(configuration.runTimeParams());
        if (getPredefinedSelection() != null) {
            bldr = bldr.predefinedColumnSelection(getPredefinedSelection(), getPredefinedSelectionVersion());
        } else if (getCustomizedSelection() != null) {
            bldr = bldr.customizedColumnSelection(getCustomizedSelection());
        } else {
            log.warn("Neither PredefinedSelection nor CustomizedSelection is provided.");
        }
        if (events.size() > 1) {
            bldr = bldr.modelName(configuration.getModelName() + " (" + currentEvent.getDisplayName() + ")");
        }
        if (configuration.getDisplayName() != null) {
            bldr = bldr.displayName(configuration.getDisplayName());
        }
        ModelingServiceExecutor modelExecutor = new ModelingServiceExecutor(bldr);
        return modelExecutor;
    }

    protected String getMetadataTableFolderName(Table eventTable, Attribute currentEvent) {
        return String.format("%s-%s-Metadata", eventTable.getName(), currentEvent.getDisplayName());
    }

}
