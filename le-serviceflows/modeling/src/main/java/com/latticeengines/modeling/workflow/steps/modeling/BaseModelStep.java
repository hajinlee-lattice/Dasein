package com.latticeengines.modeling.workflow.steps.modeling;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.modeling.PivotValuesLookup;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.ModelStepConfiguration;
import com.latticeengines.domain.exposed.util.ModelingUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

public abstract class BaseModelStep<T extends ModelStepConfiguration> extends BaseWorkflowStep<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseModelStep.class);

    @Autowired
    protected MetadataProxy metadataProxy;

    protected Table getEventTable() {
        if (executionContext.containsKey(EVENT_TABLE)) {
            Table table = getObjectFromContext(EVENT_TABLE, Table.class);
            log.info("Getting an event table from workflow context: " + table.getName());
            return table;
        } else {
            log.info("Did not see an event table in workflow context. Get one from metadata proxy for event table name "
                    + configuration.getEventTableName());
            return metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                    configuration.getEventTableName());
        }
    }

    protected Predefined getPredefinedSelection() {
        return Predefined.fromName(getStringValueFromContext(MATCH_PREDEFINED_SELECTION));
    }

    protected String getPredefinedSelectionVersion() {
        return getStringValueFromContext(MATCH_PREDEFINED_SELECTION_VERSION);
    }

    protected ColumnSelection getCustomizedSelection() {
        return getObjectFromContext(MATCH_CUSTOMIZED_SELECTION, ColumnSelection.class);
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
        if (configuration.isExpectedValue()) {
            targets.add("Revenue: " + InterfaceName.__Revenue.name());
        }
        if (configuration.isCrossSellModel()) {
            return targets.toArray(new String[targets.size()]);
        }

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
            log.info("No first name attribute in this event table.");
        }
        Attribute website = eventTable.getAttribute(InterfaceName.Website);
        if (website != null) {
            targets.add("Website: " + website.getName());
        } else {
            log.info("No website attribute in this event table.");
        }
        Attribute phoneNumber = eventTable.getAttribute(InterfaceName.PhoneNumber);
        if (phoneNumber != null) {
            targets.add("PhoneNumber: " + phoneNumber.getName());
        } else {
            log.info("No phone number attribute in this event table.");
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
            // We need to make sure that Email is available for pipeline steps
            targets.add("Email: " + email.getName());
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
        return createModelingServiceExecutor(eventTable, currentEvent, false);
    }

    protected ModelingServiceExecutor createModelingServiceExecutor(Table eventTable, Attribute currentEvent,
            boolean skipStandardTransform) throws Exception {
        ModelingServiceExecutor.Builder bldr = createModelingServiceExecutorBuilder(configuration, eventTable);

        List<String> excludedColumns = new ArrayList<>();

        List<Attribute> events = eventTable.getAttributes(LogicalDataType.Event);
        if (events.isEmpty()) {
            throw new RuntimeException("No events in event table");
        }
        for (Attribute event : events) {
            excludedColumns.add(event.getName());
        }

        log.info("Exclude prop data columns = " + configuration.getModelSummaryProvenance()
                .getBoolean(ProvenancePropertyName.ExcludePropdataColumns, false));

        if (configuration.isExpectedValue()) {
            excludedColumns.add(InterfaceName.__Revenue.name());
        }
        for (Attribute attr : eventTable.getAttributes()) {
            if (attr.getApprovedUsage() == null //
                    || attr.getApprovedUsage().size() == 0 //
                    || attr.getApprovedUsage().get(0).equals("None")) {
                excludedColumns.add(attr.getName());
            } else if (configuration.getModelSummaryProvenance()
                    .getBoolean(ProvenancePropertyName.ExcludePropdataColumns, false) && attr.getTags() != null) {
                Set<String> tags = new HashSet<>(attr.getTags());

                if (tags.contains(Tag.EXTERNAL.getName()) || tags.contains(Tag.EXTERNAL_TRANSFORM.getName())) {
                    excludedColumns.add(attr.getName());
                }
            }
            if (!(attr.getApprovedUsage() == null //
                    || attr.getApprovedUsage().size() == 0 //
                    || attr.getApprovedUsage().get(0).equals("None")) && attr.getTags() != null
                    && attr.getTags().contains(Tag.INTERNAL.getName()) && attr.getIsCoveredByOptionalRule()) {
                configuration.addProvenanceProperty(ProvenancePropertyName.ConflictWithOptionalRules, true);
            }
        }

        String[] excludeList = new String[excludedColumns.size()];
        excludedColumns.toArray(excludeList);
        bldr = bldr.profileExcludeList(excludeList);
        bldr = bldr.targets(getTargets(eventTable, currentEvent)) //
                .metadataTable(getMetadataTableFolderName(eventTable, currentEvent)) //
                .keyColumn(configuration.getIdColumnName())//
                .modelName(configuration.getModelName())//
                .jobId(jobId) //
                .eventTableName(getEventTable().getName()) //
                .sourceSchemaInterpretation(getConfiguration().getSourceSchemaInterpretation()) //
                .trainingTableName(getConfiguration().getTrainingTableName()) //
                .targetTableName(getConfiguration().getTargetTableName()) //
                .transformationGroupName(getTransformationGroupName()) //
                .skipStandardTransform(skipStandardTransform) //
                .enableV2Profiling(configuration.isV2ProfilingEnabled()) //
                .crossSellModel(configuration.isCrossSellModel()) //
                .expectedValue(configuration.isExpectedValue()) //
                .pivotArtifactPath(configuration.getPivotArtifactPath()) //
                .moduleName(configuration.getModuleName()) //
                .productType(configuration.getProductType()) //
                .setModelSummaryProvenance(configuration.getModelSummaryProvenance()) //
                .dataCloudVersion(configuration.getDataCloudVersion()) //
                .runTimeParams(configuration.getRunTimeParams());
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

    @SuppressWarnings("unused")
    private String getKeyColumn(Table eventTable) {
        String keyColumn = InterfaceName.AccountId.name();
        String[] attrNames = eventTable.getAttributeNames();
        for (String attrName : attrNames) {
            if (attrName.equals(InterfaceName.Id.name())) {
                keyColumn = InterfaceName.Id.name();
                break;
            }
        }
        return keyColumn;
    }

    private String getDataCompositionContents(Table eventTable) {
        DataComposition dataComposition = new DataComposition();
        Map.Entry<Map<String, FieldSchema>, List<TransformDefinition>> transforms = eventTable
                .getRealTimeTransformationMetadata();
        dataComposition.fields = transforms.getKey();
        dataComposition.transforms = transforms.getValue();
        return JsonUtils.serialize(dataComposition);
    }

    protected ModelingServiceExecutor.Builder createModelingServiceExecutorBuilder(
            ModelStepConfiguration modelStepConfiguration, Table eventTable) {
        String metadataContents = JsonUtils.serialize(eventTable.getModelingMetadata());
        try {
            PivotValuesLookup pivotValues = ModelingUtils.getPivotValues(yarnConfiguration,
                    modelStepConfiguration.getPivotArtifactPath());
            metadataContents = ModelingUtils.addPivotValuesToMetadataContent(eventTable.getModelingMetadata(),
                    pivotValues);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        }

        String dataCompositionContents = getDataCompositionContents(eventTable);

        List<DataRule> dataRules = null;
        if (executionContext.containsKey(DATA_RULES)) {
            dataRules = getListObjectFromContext(DATA_RULES, DataRule.class);
        } else {
            dataRules = modelStepConfiguration.getDataRules();
        }

        ModelingServiceExecutor.Builder bldr = new ModelingServiceExecutor.Builder();
        bldr.sampleSubmissionUrl("/modeling/samples") //
                .profileSubmissionUrl("/modeling/profiles") //
                .modelSubmissionUrl("/modeling/models") //
                .retrieveFeaturesUrl("/modeling/features") //
                .retrieveJobStatusUrl("/modeling/jobs/%s") //
                .retrieveModelingJobStatusUrl("/modeling/modelingjobs/%s") //
                .modelingServiceHostPort(modelStepConfiguration.getMicroServiceHostPort()) //
                .modelingServiceHdfsBaseDir(modelStepConfiguration.getModelingServiceHdfsBaseDir()) //
                .customer(modelStepConfiguration.getCustomerSpace().toString()) //
                .metadataContents(metadataContents) //
                .dataCompositionContents(dataCompositionContents) //
                .yarnConfiguration(yarnConfiguration) //
                .hdfsDirToSample(getHdfsDir(eventTable.getExtracts().get(0).getPath())) //
                .table(eventTable.getName()) //
                .modelProxy(modelProxy) //
                .jobProxy(jobProxy) //
                .dataRules(dataRules) //
                .runTimeParams(modelStepConfiguration.getRunTimeParams()) //
                .setModelSummaryProvenance(modelStepConfiguration.getModelSummaryProvenance()) //
                .productType(modelStepConfiguration.getProductType());

        return bldr;
    }

    protected String getEventFolderName(Table eventTable, Attribute currentEvent) {
        String baseEventTableName = eventTable.getName().replaceAll("[^A-Za-z0-9_-]", "_");

        /*
         * Preserving an option for history, although it appears that
         * baseEventTableName is the full folder name! String
         * baseCurrentEventName = currentEvent.getDisplayName()
         * 
         * .replaceAll("[^A-Za-z0-9_-]", "_") .replace("-Event", "");
         * 
         * return String.format("%s-%s", baseEventTableName,
         * baseCurrentEventName);
         */

        return baseEventTableName;
    }

    protected String getMetadataTableFolderName(Table eventTable, Attribute currentEvent) {
        return String.format("%s-%s-Metadata", eventTable.getName().replaceAll("[^A-Za-z0-9_-]", "_"),
                currentEvent.getDisplayName().replaceAll("[^A-Za-z0-9_-]", "_"));
    }
}
