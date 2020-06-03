package com.latticeengines.pls.service.impl;

import static com.latticeengines.pls.util.ImportWorkflowUtils.validateFieldDefinitionRequestParameters;
import static com.latticeengines.pls.util.ImportWorkflowUtils.validateFieldDefinitionsRequestBody;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FetchFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinition;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.pls.frontend.OtherTemplateData;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsRequest;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.query.EntityTypeUtils;
import com.latticeengines.domain.exposed.util.ImportWorkflowSpecUtils;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.pls.metadata.resolution.MetadataResolver;
import com.latticeengines.pls.service.CDLService;
import com.latticeengines.pls.service.DataMappingService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.util.CDLExternalSystemUtils;
import com.latticeengines.pls.util.ImportWorkflowUtils;
import com.latticeengines.pls.util.SystemIdsUtils;
import com.latticeengines.proxy.exposed.cdl.CDLExternalSystemProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.core.ImportWorkflowSpecProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("dataMappingService")
public class DataMappingServiceImpl implements DataMappingService {

    private static final Logger log = LoggerFactory.getLogger(DataMappingServiceImpl.class);

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private SourceFileService sourceFileService;

    @Inject
    private BatonService batonService;

    @Inject
    private CDLService cdlService;

    @Inject
    private ImportWorkflowSpecProxy importWorkflowSpecProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private CDLExternalSystemProxy cdlExternalSystemProxy;

    private static final String TEMPLATE_NAME = "%s_%d_Template";

    @Override
    public FetchFieldDefinitionsResponse fetchFieldDefinitions(String systemName, String systemType,
                                                               String systemObject, String importFile)
            throws Exception {

        // 1. Validate HTTP request parameters.
        validateFieldDefinitionRequestParameters("Fetch", systemName, systemType, systemObject, importFile);

        // 2. Generate extra fields from older parameters to interact with proxies and services.
        // systemObject ==> entityType
        // systemName + entityType ==> feedType
        // "File" ==> source   [hard coded for since only File is needed]
        // importFile ==> sourceFile
        // ==> customerSpace

        // 2a. Convert systemObject to entity.
        EntityType entityType = EntityType.fromDisplayNameToEntityType(systemObject);

        // 2b. Convert systemName and entityType to feedType.
        String feedType = EntityTypeUtils.generateFullFeedType(systemName, entityType);

        // 2c. Generate source string.
        String source = "File";

        // 2d. Generate sourceFile object.
        SourceFile sourceFile = StringUtils.isBlank(importFile) ? null : getSourceFile(importFile);

        // 2e. Generate customerSpace.
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();

        log.debug(String.format("Internal Values:\n   entity: %s\n   subType: %s\n   feedType: %s\n   source: %s\n" +
                        "   Customer Space: %s", entityType.getEntity(), entityType.getSubType(),
                feedType, source, customerSpace.toString()));

        // TODO(jwinter): Need to incorporate Batch Store into initial generation of FetchFieldDefinitionsResponse.
        // 3. Generate FetchFieldMappingResponse by combining:
        //    a. A FieldDefinitionRecord to hold the current field definitions.
        //    b. Spec for this system.
        //    c. Existing field definitions from DataFeedTask.
        //    d. Columns and autodetection results from the sourceFile.
        //    f. Batch Store (TODO)

        // 3a. Setup up FetchFieldDefinitionsResponse and Current FieldDefinitionsRecord.
        FetchFieldDefinitionsResponse fetchFieldDefinitionsResponse = new FetchFieldDefinitionsResponse();
        fetchFieldDefinitionsResponse.setCurrentFieldDefinitionsRecord(
                new FieldDefinitionsRecord(systemName, systemType, systemObject));

        // 3b. Retrieve Spec for given systemType and systemObject.
        fetchFieldDefinitionsResponse.setImportWorkflowSpec(
                importWorkflowSpecProxy.getImportWorkflowSpec(customerSpace.toString(), systemType, systemObject));

        // 3c. Find previously saved template matching this customerSpace, source, feedType, and entityType, if it
        // exists.
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), source, feedType,
                entityType.getEntity().name());
        printDataFeedTask("Existing DataFeedTask Template", dataFeedTask);
        Table existingTable = null;
        Map<String, FieldDefinition> existingFieldDefinitionsMap = null;
        if (dataFeedTask != null) {
            existingTable = dataFeedTask.getImportTemplate();
            fetchFieldDefinitionsResponse.setExistingFieldDefinitionsMap(
                    ImportWorkflowUtils.getFieldDefinitionsMapFromTable(existingTable));
        }

        // 3d. Create a MetadataResolver using the sourceFile if sourceFile exists.
        if (sourceFile != null) {
            MetadataResolver resolver = getMetadataResolver(sourceFile, null, true);
            fetchFieldDefinitionsResponse.setAutodetectionResultsMap(
                    ImportWorkflowUtils.generateAutodetectionResultsMap(resolver));
        }

        // 3e. Fetch all other DataFeedTask templates for Other Systems that are the same System Object and extract
        // the fieldTypes used for each field.
        List<DataFeedTask> tasks =
                dataFeedProxy.getDataFeedTaskWithSameEntityExcludeOne(customerSpace.toString(),
                        entityType.getEntity().name(), source, feedType);
        if (CollectionUtils.isNotEmpty(tasks)) {
            fetchFieldDefinitionsResponse.setOtherTemplateDataMap(ImportWorkflowUtils.generateOtherTemplateDataMap(tasks));
        }

        // 3f. Get the Metadata Attribute data from the Batch Store and get the fieldTypes set there.
        // TODO(jwinter):  Implement Batch Store extractions.

        // 4. Generate the initial FieldMappingsRecord based on the Spec, existing table, input file, and batch store.
        ImportWorkflowUtils.generateCurrentFieldDefinitionRecord(fetchFieldDefinitionsResponse);

        return fetchFieldDefinitionsResponse;
    }

    /**
     1. Required Spec (Lattice) Fields are mapped
     2. No fieldNames are mapped more than once
     3. If the standard Lattice fields are not mapped, they are included as “new” customer fields unless ignored by the user.
     4. Physical Data Type
     Compare physicalDataType against column data.
     5. Need to support special case allowed physical date type changes for backwards compatibility
     Deal with attribute name case sensitivity issues.
     6. DATE Type Validation
     Compare data and time format of DATE type against column data
     */
    @Override
    public ValidateFieldDefinitionsResponse validateFieldDefinitions(String systemName, String systemType,
                                                                     String systemObject, String importFile,
                                                                     ValidateFieldDefinitionsRequest validateRequest) {

        validateFieldDefinitionRequestParameters("Validate", systemName, systemType, systemObject, importFile);

        // get default spec from s3
        if (validateRequest.getImportWorkflowSpec() == null || MapUtils.isEmpty(validateRequest.getImportWorkflowSpec().getFieldDefinitionsRecordsMap())) {
            throw new RuntimeException(String.format("no spec info for system type %s and system object %s",
                    systemType, systemObject));
        }

        if (validateRequest.getCurrentFieldDefinitionsRecord() == null || MapUtils.isEmpty(validateRequest.getCurrentFieldDefinitionsRecord().getFieldDefinitionsRecordsMap())) {
            throw new RuntimeException("no field definition records");
        }

        Map<String, FieldDefinition> autoDetectionResultsMap = validateRequest.getAutodetectionResultsMap();
        Map<String, List<FieldDefinition>> specFieldDefinitionsRecordsMap =
                validateRequest.getImportWorkflowSpec().getFieldDefinitionsRecordsMap();
        Map<String, List<FieldDefinition>> fieldDefinitionsRecordsMap =
                validateRequest.getCurrentFieldDefinitionsRecord().getFieldDefinitionsRecordsMap();
        Map<String, FieldDefinition> existingFieldDefinitionMap = validateRequest.getExistingFieldDefinitionsMap();
        Map<String, OtherTemplateData> otherTemplateDataMap = validateRequest.getOtherTemplateDataMap();

        // 1 Generate source file and resolver
        SourceFile sourceFile = StringUtils.isBlank(importFile) ? null : getSourceFile(importFile);
        MetadataResolver resolver = sourceFile == null ? null : getMetadataResolver(sourceFile, null, true);

        // 2. generate validation message
        ValidateFieldDefinitionsResponse response =
                ImportWorkflowUtils.generateValidationResponse(fieldDefinitionsRecordsMap, autoDetectionResultsMap,
                        specFieldDefinitionsRecordsMap, existingFieldDefinitionMap, otherTemplateDataMap, resolver);
        // set field definition records map for ui
        response.setFieldDefinitionsRecordsMap(fieldDefinitionsRecordsMap);
        return response;
    }

    // TODO(jwinter): Steps being left for validation:
    //   1. Make sure the new metadata table has all the required attributes from the Spec.
    //   2. Get the set of existing templates for all system types that match this entity.
    //      a. If the existing templates have lower cased attributes names, lower case the new table’s attribute
    //         names.
    //      b. Make sure the physical types of attributes in the new table match those of the existing templates.
    //   3. Check that the new metadata table hasn't changed physicalDataTypes of attributes compared to the
    //      existing template, if DataFeed state is not DataFeed.Status.Initing.
    //   4. Final checks against Spec:
    //      a. All required attributes are there.
    //      b. Physical types may differ for special cases.  (this may no longer be applicable)
    //      c. Verify Spec field are not changed.
    @Override
    public FieldDefinitionsRecord commitFieldDefinitions(String systemName, String systemType, String systemObject,
                                                         String importFile, boolean runImport,
                                                         FieldDefinitionsRecord commitRequest)
            throws LedpException, IllegalArgumentException {

        // 1a. Validate HTTP request parameters.
        validateFieldDefinitionRequestParameters("Commit", systemName, systemType, systemObject, importFile);

        // 1b. Validate Commit Request.
        validateFieldDefinitionsRequestBody("Commit", commitRequest);

        // 2. Generate extra fields from older parameters to interact with proxies and services.
        // systemObject ==> entityType
        // systemName + entityType ==> feedType
        // "File" ==> source   [hard coded since only File is needed]
        // importFile ==> sourceFile
        // ==> customerSpace

        // 2a. Convert systemObject to entity.
        EntityType entityType = EntityType.fromDisplayNameToEntityType(systemObject);

        // 2b. Convert systemName and entityType to feedType.
        String feedType = EntityTypeUtils.generateFullFeedType(systemName, entityType);

        // 2c. Generate source string.
        String source = "File";

        // 2d. Generate sourceFile object.
        SourceFile sourceFile = StringUtils.isBlank(importFile) ? null : getSourceFile(importFile);

        // 2e. Generate customerSpace.
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();

        log.debug(String.format("Internal Values:\n   entity: %s\n   subType: %s\n   feedType: %s\n   source: %s\n" +
                        "   Customer Space: %s", entityType.getEntity(), entityType.getSubType(),
                feedType, source, customerSpace.toString()));

        // 3. Process System IDs.  For now, this is only supported for entity types Accounts, Contacts, and Leads.
        if (EntityType.Accounts.equals(entityType) || EntityType.Contacts.equals(entityType) ||
                EntityType.Leads.equals(entityType)) {
            SystemIdsUtils.processSystemIds(customerSpace, systemName, systemType, entityType, commitRequest,
                    cdlService);
        }

        // 4. Update the CDL External System data structures.
        // Set external system column name, this step will reset the field name(appending "_ID"), so the step
        // should be before generating table
        if (BusinessEntity.Account.equals(entityType.getEntity()) ||
                BusinessEntity.Contact.equals(entityType.getEntity())) {
            CDLExternalSystem cdlExternalSystem = CDLExternalSystemUtils.processOtherID(entityType, commitRequest);
            CDLExternalSystemUtils.setCDLExternalSystem(cdlExternalSystem, entityType.getEntity(),
                    cdlExternalSystemProxy);
        }

        // 5. Generate new table from FieldDefinitionsRecord.
        String newTableName;
        if (sourceFile != null) {
            newTableName = "SourceFile_" + sourceFile.getName().replace(".", "_");
        } else {
            newTableName = String.format(TEMPLATE_NAME, systemObject, Instant.now().toEpochMilli());
        }
        Table newTable = ImportWorkflowSpecUtils.getTableFromFieldDefinitionsRecord(newTableName, false, commitRequest);

        // 6. Delete old table associated with the source file from the database if it exists.
        if (sourceFile != null && StringUtils.isNotBlank(sourceFile.getTableName())) {
            metadataProxy.deleteTable(customerSpace.toString(), sourceFile.getTableName());
        }

        // 7. Associate the new table with the source file and add new table to the database.
        metadataProxy.createTable(customerSpace.toString(), newTable.getName(), newTable);
        if (sourceFile != null) {
            sourceFile.setTableName(newTable.getName());
            sourceFileService.update(sourceFile);
        }
        // 8. Create or Update the DataFeedTask
        String taskId = createOrUpdateDataFeedTask(newTable, customerSpace, source, feedType, entityType);

        // 9. Additional Steps
        // a. Update Attribute Configs.
        // b. Send email about S3 update.
        // TODO(jwinter): Do we need to add code to update the Attr Configs?
        // TODO(jwinter): Add code to send email about S3 Template change.


        // 10. If requested, submit a workflow import job for this new template.
        if (runImport && StringUtils.isNotBlank(importFile)) {
            log.info("Running import workflow job for CustomerSpace {} and task ID {} on file {}",
                    customerSpace.toString(), taskId, importFile);
            cdlService.submitS3ImportWithTemplateData(customerSpace.toString(), taskId, importFile);
        }

        // 11. Setup the Commit Response for this request.
        // Should the FieldDefinitionsRecord reflect any changes when new table is merged with
        // existing table?
        FieldDefinitionsRecord commitResponse = new FieldDefinitionsRecord();
        commitResponse.setFieldDefinitionsRecordsMap(commitRequest.getFieldDefinitionsRecordsMap());

        return commitResponse;
    }

    private SourceFile getSourceFile(String sourceFileName) {
        SourceFile sourceFile = sourceFileService.findByName(sourceFileName);
        if (sourceFile == null) {
            throw new RuntimeException(String.format("Could not locate source file with name %s", sourceFileName));
        }
        return sourceFile;
    }

    private MetadataResolver getMetadataResolver(SourceFile sourceFile, FieldMappingDocument fieldMappingDocument,
                                                 boolean cdlResolve) {
        return new MetadataResolver(sourceFile.getPath(), yarnConfiguration, fieldMappingDocument, cdlResolve, null);
    }

    private static void printDataFeedTask(String tableType, DataFeedTask dataFeedTask) {
        log.info("Print DataFeedTask containing " + tableType);
        if (dataFeedTask == null) {
            log.info("$JAW$ dataFeedTask is null");
            return;
        }
        printTableAttributes(tableType, dataFeedTask.getImportTemplate());
    }

    private static void printTableAttributes(String tableType, Table table) {
        if (table == null) {
            log.info("\n{} Table is null", tableType);
        } else {
            log.info("\n{} - Table name {} (display name {}) contains the following Attributes:\n", tableType,
                    table.getName(), table.getDisplayName());
            List<Attribute> attributes = table.getAttributes();
            if (CollectionUtils.isNotEmpty(attributes)) {
                for (Attribute attribute : attributes) {
                    log.info("   Name: " + attribute.getName() + "  Physical Data Type: " +
                            attribute.getPhysicalDataType() + "  DisplayName: " + attribute.getDisplayName());
                }
            }
        }
    }

    // Create or Update DataFeedTask with new table.
    // a. If there is an existing DataFeedTask template table.
    //   i. If the tables are identical with respect to data updated during import workflow template setup (name,
    //      displayName, physicalDataType, data/time formats, and timezone), there is no more steps to do.
    //   ii. Merge the new table and current DataFeedTask template table, adding new attributes and updating
    //       display name and data formats of old attributes.
    //   iii. Update the DataFeedTask with the new table.
    // b. If there is no existing DataFeedTask template table.
    //   i. Create a new DataFeedTask containing the new table.
    //   ii. Update the DataFeed status.

    // TODO(jwinter): Can we skip checking for an existing table, checking if the tables are identical, and
    // copying the changes into the existing table and just write the new table back?  The question is whether there
    // are fields and properties bucket values that are not related to import that have been stored in the
    // existing table's attributes?
    private String createOrUpdateDataFeedTask(Table newTable, CustomerSpace customerSpace, String source,
                                              String feedType, EntityType entityType) {
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), source, feedType,
                entityType.getEntity().name());
        if (dataFeedTask != null) {
            log.info("Found existing DataFeedTask template: {}", dataFeedTask.getTemplateDisplayName());
            Table existingTable = dataFeedTask.getImportTemplate();
            if (!TableUtils.compareMetadataTables(existingTable, newTable)) {
                Table mergedTable = TableUtils.mergeMetadataTables(existingTable, newTable);
                printTableAttributes("Merged", mergedTable);
                dataFeedTask.setImportTemplate(mergedTable);
                dataFeedProxy.updateDataFeedTask(customerSpace.toString(), dataFeedTask);
            }
        } else {
            log.info("No existing DataFeedTask template");
            dataFeedTask = new DataFeedTask();
            dataFeedTask.setUniqueId(NamingUtils.uuid("DataFeedTask"));
            dataFeedTask.setImportTemplate(newTable);
            dataFeedTask.setStatus(DataFeedTask.Status.Active);
            dataFeedTask.setEntity(entityType.getEntity().name());
            dataFeedTask.setFeedType(feedType);
            dataFeedTask.setSource(source);
            dataFeedTask.setActiveJob("Not specified");
            dataFeedTask.setSourceConfig("Not specified");
            dataFeedTask.setStartTime(new Date());
            dataFeedTask.setLastImported(new Date(0L));
            dataFeedTask.setLastUpdated(new Date());
            dataFeedTask.setSubType(entityType.getSubType());
            dataFeedTask.setTemplateDisplayName(entityType.getDefaultFeedTypeName());
            dataFeedProxy.createDataFeedTask(customerSpace.toString(), dataFeedTask);
            // TODO(jwinter): Add DropBoxService stuff.

            // TODO(jwinter): Should we be doing this DataFeed status update?
            DataFeed dataFeed = dataFeedProxy.getDataFeed(customerSpace.toString());
            if (dataFeed.getStatus().equals(DataFeed.Status.Initing)) {
                dataFeedProxy.updateDataFeedStatus(customerSpace.toString(), DataFeed.Status.Initialized.getName());
            }
        }
        return dataFeedTask.getUniqueId();
    }
}
