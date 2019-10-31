package com.latticeengines.pls.util;

import static com.latticeengines.pls.metadata.resolution.MetadataResolver.distinguishDateAndTime;
import static com.latticeengines.pls.metadata.resolution.MetadataResolver.getFundamentalTypeFromFieldType;
import static com.latticeengines.pls.metadata.resolution.MetadataResolver.getStatisticalTypeFromFieldType;
import static com.latticeengines.pls.util.ValidateFileHeaderUtils.convertFieldNameToAvroFriendlyFormat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.commons.text.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.TimeStampConvertUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.AttributeBuilder;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.frontend.FetchFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinition;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.pls.frontend.FieldValidationMessage;
import com.latticeengines.domain.exposed.pls.frontend.OtherTemplateData;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.util.AttributeUtils;
import com.latticeengines.pls.metadata.resolution.MetadataResolver;


public class ImportWorkflowUtils {
    private static final Logger log = LoggerFactory.getLogger(ImportWorkflowUtils.class);

    protected static final String USER_PREFIX = "user_";
    // TODO(jwinter): Reconsider if the Spec section for Custom Fields should be indicated in a different manner
    //     rather than hard coded.
    // String representing the section of the template reserved for non-standard customer generated fields.
    public static final String CUSTOM_FIELDS = "Custom Fields";

    private static final String Unique_ID = "Unique ID";
    private static final String OTHER_IDS = "Other IDs";
    private static final String MATCH_IDS = "Match IDs";

    protected static final String ENTITY_ACCOUNT = "Account";
    protected static final String ENTITY_CONTACT = "Contact";
    protected static final String ENTITY_TRANSACTION = "Transaction";
    protected static final String ENTITY_PRODUCT = "Product";

    protected static final String SPLIT_CHART = "_";


    // TODO(jwinter): Make sure all necessary fields are being compared.  Make sure it is ok to skip modeling fields.
    // Compare two tables and return true if they are identical and false otherwise.
    public static boolean compareMetadataTables(Table table1, Table table2) {
        if ((table1 == null || table2 == null) && table1 != table2) {
            return false;
        }
        if (!StringUtils.equals(table1.getName(), table2.getName())) {
            return false;
        }
        if (table1.getAttributes().size() != table2.getAttributes().size()) {
            return false;
        }
        HashMap<String, Attribute> table2Attrs = new HashMap<>();
        for (Attribute attr2 : table2.getAttributes()) {
            table2Attrs.put(attr2.getName(), attr2);
        }


        for (Attribute attr1 : table1.getAttributes()) {
            if (!table2Attrs.containsKey(attr1.getName())) {
                return false;
            }
            Attribute attr2 = table2Attrs.get(attr1.getName());
            if (!StringUtils.equals(attr1.getDisplayName(), attr2.getDisplayName())) {
                return false;
            }
            if (!StringUtils.equalsIgnoreCase(attr1.getPhysicalDataType(), attr2.getPhysicalDataType())) {
                return false;
            }
            // TODO(jwinter): Do we need to check required?
            if (attr1.getRequired() != attr2.getRequired()) {
                return false;
            }
            if (!StringUtils.equals(attr1.getDateFormatString(), attr2.getDateFormatString())) {
                return false;
            }
            if (!StringUtils.equals(attr1.getTimeFormatString(), attr2.getTimeFormatString())) {
                return false;
            }
            if (!StringUtils.equals(attr1.getTimezone(), attr2.getTimezone())) {
                return false;
            }
        }
        return true;
    }

    // Merge new metadata table into existing table.
    // TODO(jwinter): It is concerning that modeling fields are not copied (eg. approvedUsage, category,
    // logicalDataType).
    public static Table mergeMetadataTables(Table existingTable, Table newTable) {
        log.info("Merging table {} into table {}", newTable.getName(), existingTable.getName());

        HashMap<String, Attribute> existingAttrMap = new HashMap<>();
        for (Attribute attr : existingTable.getAttributes()) {
            existingAttrMap.put(attr.getName(), attr);
        }

        for (Attribute newAttr : newTable.getAttributes()) {
            if (!existingAttrMap.containsKey(newAttr.getName())) {
                log.info("Copying over new attribute {}", newAttr.getName());
                Attribute copyAttr = new Attribute(newAttr.getName());
                // TODO(jwinter): Does this copy both field and properties?
                AttributeUtils.copyPropertiesFromAttribute(newAttr, copyAttr);
                existingTable.addAttribute(newAttr);
            } else {
                log.info("Copying over existing attribute {}", newAttr.getName());
                // TODO(jwinter): Do we not have to copy more fields?
                // TODO(jwinter): What about the property bag?
                Attribute existingAttr = existingAttrMap.get(newAttr.getName());
                existingAttr.setDisplayName(newAttr.getDisplayName());
                // TODO(jwinter): I believe we need physicalDataType?
                existingAttr.setRequired(newAttr.getRequired());
                existingAttr.setPhysicalDataType(newAttr.getPhysicalDataType());
                existingAttr.setDateFormatString(newAttr.getDateFormatString());
                existingAttr.setTimeFormatString(newAttr.getTimeFormatString());
                existingAttr.setTimezone(newAttr.getTimezone());
                // TODO(jwinter): This is likely only for VisiDB import.
                if (newAttr.getSourceAttrName() != null) {
                    existingAttr.setSourceAttrName(newAttr.getSourceAttrName());
                }
            }
        }
        return existingTable;
    }

    public static void validateFieldDefinitionRequestParameters(
            String requestType, String systemName, String systemType, String systemObject, String importFile)
            throws LedpException {
        log.info("Field Definition Request Parameters:\n   systemName: " + systemName + "\n   systemType: " +
                systemType + "\n   systemObject: " + systemObject + "\n   importFile: " + importFile);

        // TODO(jwinter): Figure out what validation is needed.

        if (StringUtils.isBlank(systemName)) {
            log.error("systemName is null or blank");
            throw new LedpException(LedpCode.LEDP_18229, new String[]{requestType, "systemName is null/blank"});
        }

        if (StringUtils.isBlank(systemType)) {
            log.error("systemType is null or blank");
            throw new LedpException(LedpCode.LEDP_18229, new String[]{requestType, "systemType is null/blank"});
        }

        if (StringUtils.isBlank(systemObject)) {
            log.error("systemObject is null or blank");
            throw new LedpException(LedpCode.LEDP_18229, new String[]{requestType, "systemObject is null/blank"});
        }

        // Make sure systemObject maps to EntityType displayName.
        try {
            EntityType.fromDisplayNameToEntityType(systemObject);
        } catch (IllegalArgumentException e) {
            log.error("systemObject is not valid EntityType displayName");
            throw new LedpException(LedpCode.LEDP_18229, new String[]{requestType,
                    "systemObject value " + systemObject + " is not a valid EntityType"});
        }

        if (StringUtils.isBlank(importFile)) {
            log.error("importFile is null or blank");
            throw new LedpException(LedpCode.LEDP_18229, new String[]{requestType, "importFile is null/blank"});
        }
    }

    public static void validateFieldDefinitionsRequestBody(String requestType, FieldDefinitionsRecord record) {
        // Make sure that the field definitions request has FieldDefinitionsRecord section.
        if (record == null || MapUtils.isEmpty(record.getFieldDefinitionsRecordsMap())) {
            log.error("{} FieldDefinitionsRecord request is null or missing FieldDefinitionsRecord map", requestType);
            throw new LedpException(LedpCode.LEDP_18230, new String[]{requestType,
                    "FieldDefinitionsRecord null or FieldDefinitionsRecordsMap null or empty"});
        }
    }

    // TODO(jwinter): This is copied from test code and likely a hack.  Find a better way of doing this.
    public static String getFeedTypeFromSystemNameAndEntityType(String system, EntityType entityType) {
        return system + SPLIT_CHART + entityType.getDefaultFeedTypeName();
    }

    public static Map<String, FieldDefinition> getFieldDefinitionsMapFromTable(Table table) {
        if (table == null) {
            log.warn("Tried to created FieldDefinitionsMap from null Table");
            return null;
        }
        log.info("Creating FieldDefinitionsMap from Table {} for Tenant {}:", table.getName(),
                table.getTenant() != null ? table.getTenant().getName() : "null");
        Map<String, FieldDefinition> map = new HashMap<>();
        for (Attribute attribute : table.getAttributes()) {
            map.put(attribute.getName(), getFieldDefinitionFromAttribute(attribute));
            log.info("    Adding fieldName " + attribute.getName() + " to existing field map");
        }
        return map;
    }

    public static Map<String, FieldDefinition> generateAutodetectionResultsMap(MetadataResolver resolver) {
        if (resolver == null) {
            throw new IllegalArgumentException("MetadataResolver cannot be null");
        }
        Map<String, FieldDefinition> map = new HashMap<>();
        // Get column header names from imported file.
        Set<String> columnHeaderNames = resolver.getHeaderFields();

        // DEBUG
        String existingColumnHeaders = "Import CSV column header names are:\n";
        for (String columnHeaderName : columnHeaderNames) {
            existingColumnHeaders += "    " + columnHeaderName + "\n";
        }
        log.info(existingColumnHeaders);
        // END DEBUG

        for (String columnHeaderName : columnHeaderNames) {
            FieldDefinition definition = new FieldDefinition();
            setFieldTypeFromColumnContent(resolver, columnHeaderName, definition);
            definition.setColumnName(StringEscapeUtils.escapeHtml4(columnHeaderName));
            log.info("Putting columnName {} into autodetected map with type {}", definition.getColumnName(),
                    definition.getFieldType().getFieldType());
            map.put(definition.getColumnName(), definition);
        }
        return map;
    }

    private static Attribute getAttributeFromFieldDefinition(FieldDefinition definition) {
        if (StringUtils.isBlank(definition.getFieldName()) && StringUtils.isBlank(definition.getColumnName())) {
            throw new IllegalArgumentException(
                    "FieldDefinition cannot have both fieldName and columnName null or empty");
        }

        if (definition.getFieldType() == null) {
            throw new IllegalArgumentException("FieldDefinition with fieldName " + definition.getFieldName() +
                    "and columnName " + definition.getColumnName() + " must have non-null fieldType.");
        }

        // TODO(jwinter): Re-evaluate if the settings below are correct.  In particular, how name, displayName,
        //     and interfaceName are set.
        String attrName;
        InterfaceName interfaceName = null;
        // A blank field name means that it's a new custom field.  Generate a field name from the column name.
        if (StringUtils.isBlank(definition.getFieldName())) {
            attrName = ValidateFileHeaderUtils.convertFieldNameToAvroFriendlyFormat(definition.getColumnName());
            attrName = USER_PREFIX + attrName;
        } else {
            attrName = ValidateFileHeaderUtils.convertFieldNameToAvroFriendlyFormat(definition.getFieldName());
            try {
                interfaceName = InterfaceName.valueOf(definition.getFieldName());
            } catch (IllegalArgumentException e) {
                interfaceName = null;
            }
        }

        return new AttributeBuilder()
                .name(attrName)
                .displayName(definition.getColumnName())
                // Tag and nullable seem to be set the same way for all Atlas Attributes.
                // TODO(jwinter): Do we need to set the tag?  Looks like it is only for legacy systems.
                //.tag(Tag.INTERNAL.toString()) //
                .nullable(true) //
                // TODO(jwinter): Does the Attribute physicalDataType String have to be lower case?  This is not
                //     consistent in the code.
                .physicalDataType(definition.getFieldType().getAvroType()) //
                .interfaceName(interfaceName) //
                .dateFormatString(definition.getDateFormat()) //
                .timeFormatString(definition.getTimeFormat()) //
                .timezone(definition.getTimeZone()) //
                // TODO(jwinter): Determine if we need this.
                .allowedDisplayNames(definition.getMatchingColumnNames()) //
                .required(definition.isRequired()) //
                // TODO(jwinter): Confirm we need to pass the modeling fields.
                .approvedUsage(definition.getApprovedUsage()) //
                .logicalDataType(definition.getLogicalDataType()) //
                .fundamentalType(definition.getFundamentalType() != null ?
                        definition.getFundamentalType().getName() : null)
                .statisticalType(definition.getStatisticalType()) //
                .category(definition.getCategory()) //
                .subcategory(definition.getSubcategory()) //
                // TODO(jwinter): Do we need to set these other fields?
                //.failImportValidator()
                //.defaultValueStr("")
                .build();
    }

    private static FieldDefinition getFieldDefinitionFromAttribute(Attribute attribute) {
        FieldDefinition definition = new FieldDefinition();
        definition.setFieldName(attribute.getName());
        definition.setFieldType(MetadataResolver.getFieldTypeFromPhysicalType(
                attribute.getPhysicalDataType()));
        definition.setColumnName(attribute.getDisplayName());
        definition.setDateFormat(attribute.getDateFormatString());
        definition.setTimeFormat(attribute.getTimeFormatString());
        definition.setTimeZone(attribute.getTimezone());
        definition.setMatchingColumnNames(attribute.getAllowedDisplayNames());
        definition.setRequired(attribute.getRequired());
        definition.setApprovedUsage(attribute.getApprovedUsage());
        definition.setLogicalDataType(attribute.getLogicalDataType());
        try {
            definition.setFundamentalType(FundamentalType.fromName(attribute.getFundamentalType()));
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse FundamentalType " + attribute.getFundamentalType() +
                    " when converting Attribute " + attribute.getName() + " to FieldDefinition");
        }
        definition.setStatisticalType(attribute.getStatisticalType());
        definition.setCategory(attribute.getCategory());
        definition.setSubcategory(attribute.getSubcategory());
        return definition;
    }

    private static FieldDefinition copyFieldDefinition(FieldDefinition definition1) {
        FieldDefinition definition2 = new FieldDefinition();
        definition2.setFieldName(definition1.getFieldName());
        definition2.setColumnName(definition1.getColumnName());
        definition2.setInCurrentImport(definition1.isInCurrentImport());
        definition2.setFieldType(definition1.getFieldType());
        definition2.setRequired(definition1.isRequired());
        definition2.setApprovedUsage(definition1.getApprovedUsage());
        definition2.setLogicalDataType(definition1.getLogicalDataType());
        definition2.setFundamentalType(definition1.getFundamentalType());
        definition2.setStatisticalType(definition1.getStatisticalType());
        definition2.setCategory(definition1.getCategory());
        definition2.setSubcategory(definition1.getSubcategory());
        return definition2;
    }

    public static FieldDefinitionsRecord createFieldDefinitionsRecordFromSpecAndTable(
            ImportWorkflowSpec spec, Table existingTable, MetadataResolver resolver) {
        FieldDefinitionsRecord fieldDefinitionsRecord = new FieldDefinitionsRecord();
        // For now, if the Spec is null, return an empty result.
        if (spec == null) {
            return fieldDefinitionsRecord;
        }

        // Get column header names from imported file.
        Set<String> columnHeaderNames = resolver.getHeaderFields();

        // DEBUG
        String existingColumnHeaders = "Existing column headers are:\n";
        for (String headerName : columnHeaderNames) {
            existingColumnHeaders += "    " + headerName + "\n";
        }
        log.info(existingColumnHeaders);

        // Create a data structure to hold FieldDefinitions based on existing template Attributes until the Spec is
        // processed.
        // Map<fieldName, Pair<specSectionName, FieldDefinition>>
        Map<String, FieldDefinition> existingFieldNameToDefinitionMap = new HashMap<>();

        if (existingTable != null) {
            for (Attribute attribute : existingTable.getAttributes()) {
                FieldDefinition existingDefinition = getFieldDefinitionFromAttribute(attribute);
                // If the import file column headers contains the existing attribute's display name, mark this field as
                // in the current import and remove it from the set of available matching column headers.  This needs
                // to be done before examining the Spec to prevent any other Spec fields from claiming this column.
                if (columnHeaderNames.contains(attribute.getDisplayName())) {
                    log.info("Existing field " + existingDefinition.getFieldName() + " matched column " +
                            attribute.getDisplayName());
                    existingDefinition.setInCurrentImport(true);
                    columnHeaderNames.remove(attribute.getDisplayName());
                } else {
                    existingDefinition.setInCurrentImport(false);
                }
                // Store FieldDefinitions in a easy to look up data structure, keyed by FieldName.
                existingFieldNameToDefinitionMap.put(existingDefinition.getFieldName(), existingDefinition);
                log.info("Adding fieldName " + existingDefinition.getFieldName() + " to existing field map");
            }
        }

        // Iteration through all the sections in the Spec.
        log.info("Processing Spec for SystemType {} and SystemObject {}", spec.getSystemType(), spec.getSystemObject());
        for (Map.Entry<String, List<FieldDefinition>> section : spec.getFieldDefinitionsRecordsMap().entrySet()) {
            if (CollectionUtils.isEmpty(section.getValue())) {
                fieldDefinitionsRecord.addFieldDefinitionsRecords(section.getKey(), new ArrayList<>(), false);
                continue;
            }

            // Iterate through each field definition in the Spec section.
            for (FieldDefinition specDefinition : section.getValue()) {
                validateSpecFieldDefinition(section.getKey(), specDefinition);

                // Convert Spec fieldNames to Avro friendly format if necessary.
                String avroFieldName = convertFieldNameToAvroFriendlyFormat(specDefinition.getFieldName());
                if (!avroFieldName.equals(specDefinition.getFieldName())) {
                    log.warn("Found non-Avro compatible Spec fieldName {} in section {}", specDefinition.getFieldName(),
                            section.getKey());
                    specDefinition.setFieldName(avroFieldName);
                }

                FieldDefinition recordDefinition = null;
                if (existingFieldNameToDefinitionMap.containsKey(specDefinition.getFieldName())) {
                    log.info("Found matching existing field for fieldName " + specDefinition.getFieldName());
                    // If the Spec FieldDefinition field name matches an existing FieldDefinition, use the existing
                    // definition for this record.
                    recordDefinition = existingFieldNameToDefinitionMap.get(specDefinition.getFieldName());
                    // Remove this existing FieldDefinition from the Hash Map of existing FieldDefinitions.
                    existingFieldNameToDefinitionMap.remove(specDefinition.getFieldName());
                } else {
                    log.info("Creating new field for fieldName " + specDefinition.getFieldName());
                    // If this Spec FieldDefinition is not in the existing template, build the record from the Spec.
                    recordDefinition = copyFieldDefinition(specDefinition);

                    // Iterate through all the column header names checking if any match the set of accepted names for
                    // the Spec's FieldDefinition.
                    boolean foundMatchingColumn = false;
                    Iterator<String> columnIterator = columnHeaderNames.iterator();
                    while (columnIterator.hasNext()) {
                        String columnName = columnIterator.next();
                        // Check if the standardized column name matches any of the standardized accepted names for this
                        // field.
                        if (doesColumnNameMatch(columnName, specDefinition.getMatchingColumnNames())) {
                            log.info("Existing field " + recordDefinition.getFieldName() + " matched column " +
                                    columnName);

                            foundMatchingColumn = true;
                            recordDefinition.setColumnName(columnName);
                            columnIterator.remove();

                            // If there is a match and this field is a date type, autodetect the date and time formats and
                            // time zone from the column data.
                            if (UserDefinedType.DATE.equals(recordDefinition.getFieldType())) {
                                List<String> columnDataFields = resolver.getColumnFieldsByHeader(columnName);
                                MutableTriple<String, String, String> dateTimeZone =
                                        distinguishDateAndTime(columnDataFields);
                                if (dateTimeZone != null) {
                                    recordDefinition.setDateFormat(dateTimeZone.getLeft());
                                    recordDefinition.setTimeFormat(dateTimeZone.getMiddle());
                                    recordDefinition.setTimeZone(dateTimeZone.getRight());
                                }
                            }
                            break;
                        }
                    }
                    recordDefinition.setInCurrentImport(foundMatchingColumn);
                }
                // Add screen name to record FieldDefinition.
                recordDefinition.setScreenName(specDefinition.getScreenName());

                // Add the FieldDefinition to the FieldDefinitionRecord regardless of whether a match was found
                // among the column header names.
                addFieldDefinitionToRecord(recordDefinition, section.getKey(), fieldDefinitionsRecord);
            }
        }

        // Iterate through the remaining existing template FieldDefinitions, which did not match any field in the
        // Spec and add them to the FieldDefinitionRecord as Custom Fields.
        log.info("Add existing Custom Fields to FieldDefinitionsRecord:");
        for (FieldDefinition existingDefinition : existingFieldNameToDefinitionMap.values()) {
            addFieldDefinitionToRecord(existingDefinition, CUSTOM_FIELDS, fieldDefinitionsRecord);
        }

        String schemaInterpretationString = getSchemaInterpretationFromSpec(spec).name();

        // Iterate through the remaining column header names, which did not match any field in the Spec and add them
        // to the FieldDefinitionRecord as Custom Fields.
        log.info("Add new Custom Fields to FieldDefinitionsRecord:");
        for (String columnName : columnHeaderNames) {
            FieldDefinition recordDefinition = createNewCustomFieldDefinition(resolver, columnName,
                    schemaInterpretationString);
            addFieldDefinitionToRecord(recordDefinition, CUSTOM_FIELDS, fieldDefinitionsRecord);
        }
        return fieldDefinitionsRecord;
    }

    // Create the initial currentFieldDefinitionsRecord which will be used as the starting point recommendation for the
    // field mapping for the user.
    public static void generateCurrentFieldDefinitionRecord(FetchFieldDefinitionsResponse fetchResponse) {
        validateFetchFieldDefinitionsResponse(fetchResponse);

        // Create a FieldDefinitionsRecord to hold the mapping recommendations to be generated based on the import CSV,
        // the Spec, and the existing template for this System Name, System Type, and System Object.
        FieldDefinitionsRecord record = new FieldDefinitionsRecord();

        // Generate the set of column header names from imported file, which can be found as the keys of the
        // autodetection map.  This set will track the columns that are available to map to either new Lattice Fields
        // or Custom Fields in this import, after removing any column header names mapped to fields in the existing
        // template.
        Map<String, FieldDefinition> autodetectionMap = fetchResponse.getAutodetectionResultsMap();
        Set<String> columnHeaderNamesForNewFields = new HashSet<>(autodetectionMap.keySet());
        // Iterate through the existing template FieldDefinitions and remove any matching column names from the set
        // of column header names to prevent them from being mapped again to new Lattice Field or Custom Fields for this
        // import.
        Map<String, FieldDefinition> existingMap = fetchResponse.getExistingFieldDefinitionsMap();
        for (FieldDefinition existingDefinition : existingMap.values()) {
            if (columnHeaderNamesForNewFields.contains(existingDefinition.getColumnName())) {
                log.info("Existing field " + existingDefinition.getFieldName() + " matched imported column " +
                        existingDefinition.getColumnName());
                columnHeaderNamesForNewFields.remove(existingDefinition.getColumnName());
            }
        }
        // Create a set of the existing template FieldDefinitions to track which existing template fields map to
        // fields in the Spec.  Some fields in the existing template could be custom fields and need to be added to
        // that section in the FieldDefintionsRecord.  Further, if the Spec has changed and fields have been removed,
        // there could be fields in the existing template which no longer map to Lattice Fields and must be set as
        // custom fields in the record of FieldDefinition recommendations.
        Set<String> existingFieldNamesToBeMapped = new HashSet<>(existingMap.keySet());

        // Iteration through all the sections in the Spec.
        ImportWorkflowSpec spec = fetchResponse.getImportWorkflowSpec();
        log.info("Processing Spec for SystemType {} and SystemObject {}", spec.getSystemType(), spec.getSystemObject());
        for (Map.Entry<String, List<FieldDefinition>> section : spec.getFieldDefinitionsRecordsMap().entrySet()) {
            if (CollectionUtils.isEmpty(section.getValue())) {
                record.addFieldDefinitionsRecords(section.getKey(), new ArrayList<>(), false);
                continue;
            }

            // Iterate through each field definition in the Spec section.
            for (FieldDefinition specDefinition : section.getValue()) {
                validateSpecFieldDefinition(section.getKey(), specDefinition);

                // Convert Spec fieldNames to Avro friendly format if necessary.
                String avroFieldName = convertFieldNameToAvroFriendlyFormat(specDefinition.getFieldName());
                if (!avroFieldName.equals(specDefinition.getFieldName())) {
                    log.warn("Found non-Avro compatible Spec fieldName {} in section {}", specDefinition.getFieldName(),
                            section.getKey());
                    specDefinition.setFieldName(avroFieldName);
                }

                FieldDefinition recordDefinition = null;
                if (existingFieldNamesToBeMapped.contains(specDefinition.getFieldName())) {
                    // If this Spec field was found among the fields in the existing template, create a recommendation
                    // for this FieldDefinition based on the existing template field, with autodetected date format
                    // data if necessary.
                    log.info("Found matching existing field for fieldName " + specDefinition.getFieldName());
                    FieldDefinition existingDefinition = existingMap.get(specDefinition.getFieldName());
                    // Note, the columnHeaderNamesForNewFields set is not checked first before trying to get the
                    // FieldDefinition from the autodetection map.  This allows multiple fields from the existing
                    // template to get data from the same autodetected column since more than once field can map to
                    // the same column.
                    // TODO(jwinter): Need to add copying of external system fields here, once that part has been
                    // figured out.
                    recordDefinition = createDefinitionFromExisting(existingDefinition,
                            specDefinition.getScreenName(), specDefinition.isRequired(),
                            autodetectionMap.get(existingDefinition.getColumnName()));
                    existingFieldNamesToBeMapped.remove(specDefinition.getFieldName());
                } else {
                    // If the Spec field is not in the existing template, check if there is a column in the import whose
                    // header matches one of the matching column names for this field.  If so, create a FieldDefinition
                    // based on Spec and autodetected data about the column.  If not, create a stub FieldDefinition
                    // that is not mapped to a column but may get mapped by the user.
                    log.info("Creating new field for fieldName " + specDefinition.getFieldName());
                    String columnName = findMatchingColumnName(specDefinition.getFieldName(),
                            columnHeaderNamesForNewFields, specDefinition.getMatchingColumnNames());
                    recordDefinition = createDefinitionFromSpec(specDefinition, autodetectionMap.get(columnName));
                }
                // Add the FieldDefinition to the FieldDefinitionRecord for the field definition recommendations.
                addFieldDefinitionToRecord(recordDefinition, section.getKey(), record);
            }
        }

        // Iterate through the remaining existing template FieldDefinitions, which did not match any field in the
        // Spec and add them to the FieldDefinitionRecord as Custom Fields.
        log.info("Add existing Custom Fields to FieldDefinitionsRecord:");
        for (String fieldName : existingFieldNamesToBeMapped) {
            FieldDefinition recordDefinition = createDefinitionFromExisting(existingMap.get(fieldName), null,
                    Boolean.FALSE,
                    autodetectionMap.get(existingMap.get(fieldName).getColumnName()));
            addFieldDefinitionToRecord(recordDefinition, CUSTOM_FIELDS, record);
        }

        // Iterate through the remaining column header names, which did not match any field in the Spec, and add them
        // to the FieldDefinitionRecord as Custom Fields.
        log.info("Add new Custom Fields to FieldDefinitionsRecord:");
        for (String columnName : columnHeaderNamesForNewFields) {
            FieldDefinition recordDefinition = createNewCustomFieldDefinition(autodetectionMap.get(columnName));
            addFieldDefinitionToRecord(recordDefinition, CUSTOM_FIELDS, record);
        }

        // Set the recommend FieldDefinitionsRecord in the fetch response to the record just created.
        fetchResponse.setCurrentFieldDefinitionsRecord(record);
    }

    private static void validateFetchFieldDefinitionsResponse(FetchFieldDefinitionsResponse fetchResponse) {
        if (fetchResponse == null) {
            throw new IllegalArgumentException(
                    "FetchFieldDefinitionsResponse is null.  Can't generate Field Mappings.");
        } else if (fetchResponse.getImportWorkflowSpec() == null) {
            throw new IllegalArgumentException("ImportWorkflowSpec is null.  Can't generate Field Mappings.");
        } else if (fetchResponse.getExistingFieldDefinitionsMap() == null) {
            throw new IllegalArgumentException("ExistingFieldDefinitionsMap is null.  Can't generate Field Mappings.");
        } else if (fetchResponse.getAutodetectionResultsMap() == null) {
            throw new IllegalArgumentException("AutodetectionResultsMap is null.  Can't generate Field Mappings.");
        } else if (fetchResponse.getOtherTemplateDataMap() == null) {
            throw new IllegalArgumentException("OtherTemplateDataMap is null.  Can't generate Field Mappings.");
        }

        if (fetchResponse.getImportWorkflowSpec().getFieldDefinitionsRecordsMap().isEmpty()) {
            log.warn("FetchFieldDefinitionsResponse has ImportWorkflowSpec with empty map");
        }
        if (fetchResponse.getAutodetectionResultsMap().isEmpty()) {
            log.warn("FetchFieldDefinitionsResponse has AutodetectionResults with empty Map");
        }
    }

    private static void validateSpecFieldDefinition(String sectionName, FieldDefinition definition) {
        if (definition == null) {
            log.error("During spec iteration, found null FieldDefinition in section " + sectionName);
            throw new IllegalArgumentException("During spec iteration, found null FieldDefinition in section " +
                    sectionName);
        } else if (StringUtils.isBlank(definition.getFieldName())) {
            log.error("During spec iteration, found FieldDefinition with null fieldName in section " + sectionName);
            throw new IllegalArgumentException(
                    "During spec iteration, found FieldDefinition with null fieldName in section " + sectionName);
        } else if (CollectionUtils.isEmpty(definition.getMatchingColumnNames())) {
            log.error("During spec iteration, found FieldDefinition with null/empty matchingColumnNames in section "
                    + sectionName);
            throw new IllegalArgumentException(
                    "During spec iteration, found FieldDefinition with null/empty matchingColumnNames in section "
                            + sectionName);
        }
        log.info("    Spec section: " + sectionName + "  fieldName: " + definition.getFieldName());
    }

    // TODO(jwinter): Need to add copying of external system fields here, once that part has been figured out.
    private static FieldDefinition createDefinitionFromExisting(FieldDefinition existingDefinition,
                                                                String screenName,
                                                                Boolean isRequired,
                                                                FieldDefinition autodetectedDefinition) {
        FieldDefinition definition = new FieldDefinition();
        definition.setFieldName(existingDefinition.getFieldName());
        definition.setFieldType(existingDefinition.getFieldType());
        definition.setScreenName(screenName);
        definition.setColumnName(existingDefinition.getColumnName());
        definition.setRequired(isRequired);
        if (UserDefinedType.DATE.equals(definition.getFieldType())) {
            definition.setDateFormat(existingDefinition.getDateFormat());
            definition.setTimeFormat(existingDefinition.getTimeFormat());
            definition.setTimeZone(existingDefinition.getTimeZone());
        }
        if (autodetectedDefinition != null) {
            definition.setInCurrentImport(true);
            if (UserDefinedType.DATE.equals(definition.getFieldType())) {
                if (StringUtils.isBlank(definition.getDateFormat())) {
                    definition.setDateFormat(autodetectedDefinition.getDateFormat());
                }
                if (StringUtils.isBlank(definition.getTimeFormat())) {
                    definition.setTimeFormat(autodetectedDefinition.getTimeFormat());
                }
                if (StringUtils.isBlank(definition.getTimeZone())) {
                    definition.setTimeZone(autodetectedDefinition.getTimeZone());
                }
            }
        } else {
            definition.setInCurrentImport(false);
        }
        return definition;
    }

    private static String findMatchingColumnName(String fieldName, Set<String> columnHeaderNamesForNewFields,
                                                 List<String> matchingColumnNames) {
        Iterator<String> columnIterator = columnHeaderNamesForNewFields.iterator();
        while (columnIterator.hasNext()) {
            String columnName = columnIterator.next();
            // Check if the standardized column name matches any of the standardized accepted names for this field.
            if (doesColumnNameMatch(columnName, matchingColumnNames)) {
                log.info("Existing field " + fieldName + " matched column " + columnName);
                columnIterator.remove();
                return columnName;
            }
        }
        return null;
    }

    private static boolean doesColumnNameMatch(String columnName, List<String> matchingColumnNames) {
        if (CollectionUtils.isNotEmpty(matchingColumnNames)) {
            String standardizedColumnName = MetadataResolver.standardizeAttrName(columnName);
            String matchedColumnName = matchingColumnNames.stream() //
                    .filter(allowedName -> MetadataResolver.standardizeAttrName(allowedName)
                            .equalsIgnoreCase(standardizedColumnName)) //
                    .findFirst().orElse(null);
            return StringUtils.isNotBlank(matchedColumnName);
        }
        return false;
    }

    private static FieldDefinition createDefinitionFromSpec(FieldDefinition specDefinition,
                                                            FieldDefinition autodetectedDefinition) {
        FieldDefinition definition = new FieldDefinition();
        definition.setFieldName(specDefinition.getFieldName());
        definition.setFieldType(specDefinition.getFieldType());
        definition.setScreenName(specDefinition.getScreenName());
        definition.setRequired(specDefinition.isRequired());
        if (autodetectedDefinition != null) {
            definition.setColumnName(autodetectedDefinition.getColumnName());
            definition.setInCurrentImport(true);
            if (UserDefinedType.DATE.equals(definition.getFieldType())) {
                definition.setDateFormat(autodetectedDefinition.getDateFormat());
                definition.setTimeFormat(autodetectedDefinition.getTimeFormat());
                definition.setTimeZone(autodetectedDefinition.getTimeZone());
            }
        } else {
            definition.setInCurrentImport(false);
        }
        return definition;
    }

    private static FieldDefinition createNewCustomFieldDefinition(FieldDefinition autodetectedDefinition) {
        FieldDefinition definition = new FieldDefinition();
        definition.setFieldName(autodetectedDefinition.getFieldName());
        definition.setFieldType(autodetectedDefinition.getFieldType());
        definition.setColumnName(autodetectedDefinition.getColumnName());
        definition.setInCurrentImport(true);
        if (UserDefinedType.DATE.equals(definition.getFieldType())) {
            definition.setDateFormat(autodetectedDefinition.getDateFormat());
            definition.setTimeFormat(autodetectedDefinition.getTimeFormat());
            definition.setTimeZone(autodetectedDefinition.getTimeZone());
        }
        return definition;
    }

    private static UserDefinedType getFieldTypeFromColumnContent(FieldDefinition fieldDefinition,
                                                                 MetadataResolver resolver) {
        UserDefinedType userDefinedType;
        List<String> columnFields = resolver.getColumnFieldsByHeader(fieldDefinition.getColumnName());
        MutableTriple<String, String, String> dateTimeZone;
        if (columnFields.isEmpty()) {
            userDefinedType = UserDefinedType.TEXT;
        } else if (MetadataResolver.isBooleanTypeColumn(columnFields)) {
            userDefinedType = UserDefinedType.BOOLEAN;
        } else if (MetadataResolver.isIntegerTypeColumn(columnFields)) {
            userDefinedType = UserDefinedType.INTEGER;
        } else if (MetadataResolver.isDoubleTypeColumn(columnFields)) {
            userDefinedType = UserDefinedType.NUMBER;
        } else if ((dateTimeZone = distinguishDateAndTime(columnFields)) != null) {
            userDefinedType = UserDefinedType.DATE;
            fieldDefinition.setDateFormat(dateTimeZone.getLeft());
            fieldDefinition.setTimeFormat(dateTimeZone.getMiddle());
            fieldDefinition.setTimeZone(dateTimeZone.getRight());
        } else {
            userDefinedType = UserDefinedType.TEXT;
        }
        return userDefinedType;
    }

    private static void setFieldTypeFromColumnContent(MetadataResolver resolver, String columnHeaderName,
                                                      FieldDefinition fieldDefinition) {
        List<String> columnFields = resolver.getColumnFieldsByHeader(columnHeaderName);
        MutableTriple<String, String, String> dateTimeZone;
        if (columnFields.isEmpty()) {
            fieldDefinition.setFieldType(UserDefinedType.TEXT);
        } else if (MetadataResolver.isBooleanTypeColumn(columnFields)) {
            fieldDefinition.setFieldType(UserDefinedType.BOOLEAN);
        } else if (MetadataResolver.isIntegerTypeColumn(columnFields)) {
            fieldDefinition.setFieldType(UserDefinedType.INTEGER);
        } else if (MetadataResolver.isDoubleTypeColumn(columnFields)) {
            fieldDefinition.setFieldType(UserDefinedType.NUMBER);
        } else if ((dateTimeZone = distinguishDateAndTime(columnFields)) != null) {
            fieldDefinition.setFieldType(UserDefinedType.DATE);
            fieldDefinition.setDateFormat(dateTimeZone.getLeft());
            fieldDefinition.setTimeFormat(dateTimeZone.getMiddle());
            fieldDefinition.setTimeZone(dateTimeZone.getRight());
        } else {
            fieldDefinition.setFieldType(UserDefinedType.TEXT);
        }
    }

    private static void addFieldDefinitionToRecord(FieldDefinition definition, String section,
                                                   FieldDefinitionsRecord record) {
        if (!record.addFieldDefinition(section, definition, false)) {
            log.error("Could not add FieldDefinition with fieldName " + definition.getFieldName() +
                    " to section " + section + " because of existing record");
            throw new IllegalArgumentException("Could not add FieldDefinition with fieldName " +
                    definition.getFieldName() + " to section " + section + " because of existing record");
        }
        log.info("    Successfully added FieldDefinition with fieldName {} and columnName {} to section {}",
                definition.getFieldName(), definition.getColumnName(), section);
    }

    private static FieldDefinition createNewCustomFieldDefinition(MetadataResolver resolver, String columnHeaderName,
                                                                  String schemaInterpretationString) {
        FieldDefinition definition = new FieldDefinition();
        setFieldTypeFromColumnContent(resolver, columnHeaderName, definition);
        definition.setColumnName(StringEscapeUtils.escapeHtml4(columnHeaderName));
        definition.setInCurrentImport(true);
        definition.setRequired(false);
        definition.setApprovedUsage(Arrays.asList(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE));
        definition.setLogicalDataType(definition.getFieldType() == UserDefinedType.DATE ?
                LogicalDataType.Date : null);
        String fieldTypeString = definition.getFieldType().getAvroType().toString().toLowerCase();
        definition.setFundamentalType(
                FundamentalType.fromName(getFundamentalTypeFromFieldType(fieldTypeString)));
        definition.setStatisticalType(getStatisticalTypeFromFieldType(fieldTypeString));
        definition.setCategory(MetadataResolver.getCategoryBasedOnSchemaType(schemaInterpretationString));
        return definition;
    }

    // Generate a table from a FieldDefinitionsRecord.  The writeAllDefinitions flag defines whether FieldDefinitions
    // that haven't ever matched an import file column should be written back.  The typical usage of this flag is to
    // set it true when converting a Spec to a Table, since in that case all Spec fields should be written to the
    // Table.  It should be set false when converting a FieldDefinitionsRecord to a Table that will be written to
    // DataFeedTask template because Spec fields that were never matched to input columns should not be in the
    // Metadata Attributes table.
    public static Table getTableFromFieldDefinitionsRecord(FieldDefinitionsRecord record, boolean writeAllDefinitions) {
        Table table = new Table();

        if (record == null || MapUtils.isEmpty(record.getFieldDefinitionsRecordsMap())) {
            log.warn("getTableFromFieldDefinitionsRecord provided with null record or empty record map");
            return table;
        }

        for (Map.Entry<String, List<FieldDefinition>> entry : record.getFieldDefinitionsRecordsMap().entrySet()) {
            if (entry.getValue() == null) {
                log.warn("Section name {} has null FieldDefinitions list.", entry.getKey());
                continue;
            }

            for (FieldDefinition definition : entry.getValue()) {
                if (definition == null) {
                    log.error("During spec iteration, found null FieldDefinition in section " + entry.getKey());
                    throw new IllegalArgumentException("During spec iteration, found null FieldDefinition in section " +
                            entry.getKey());
                }

                // If writeAllDefinitions is false, only write back FieldDefinitions with columnName set back to the
                // table as this indicates that they have a current mapping column or had in the a previous import.
                // In this case, skip FieldDefinitions that don't have columnName set as these are Spec fields that
                // do not currently or did not previously match a import file column.
                if (writeAllDefinitions || (StringUtils.isNotBlank(definition.getColumnName()) &&
                        !Boolean.TRUE.equals(definition.getIgnored()))) {
                    // ignored fields should be ignored when generating table
                    Attribute attribute = getAttributeFromFieldDefinition(definition);
                    table.addAttribute(attribute);
                    log.info("   SectionName: {}  FieldName: {}  ColumnName: {}", entry.getKey(),
                            definition.getFieldName(), definition.getColumnName());
                } else {
                    log.info("   Skipped Field: SectionName: {}  FieldName: {}, Skipped state: {}", entry.getKey(),
                            definition.getFieldName(), definition.getIgnored());
                }
            }
        }
        return table;
    }

    public static SchemaInterpretation getSchemaInterpretationFromSpec(ImportWorkflowSpec spec) {
        if (StringUtils.isBlank(spec.getSystemObject())) {
            throw new IllegalArgumentException("Spec is missing SystemObject field");
        }
        EntityType entityType = EntityType.fromDisplayNameToEntityType(spec.getSystemObject());
        return SchemaInterpretation.getByName(entityType.getEntity().name());
    }

    // UNUSED FOR NOW
    public static FieldDefinitionsRecord createFieldDefinitionsRecordFromSpec(
            ImportWorkflowSpec spec, MetadataResolver resolver) {
        FieldDefinitionsRecord fieldDefinitionsRecord = new FieldDefinitionsRecord();
        if (spec == null) {
            return fieldDefinitionsRecord;
        }

        // Get column header names from imported file.
        Set<String> columnHeaderNames = resolver.getHeaderFields();
        // Track all the column header names that match with field definitions from the Spec.
        Set<String> matchedColumnNames = new HashSet<>();

        // Iteration through all the sections in the Spec.
        for (Map.Entry<String, List<FieldDefinition>> section : spec.getFieldDefinitionsRecordsMap().entrySet()) {
            if (CollectionUtils.isEmpty(section.getValue())) {
                fieldDefinitionsRecord.addFieldDefinitionsRecords(section.getKey(), new ArrayList<>(), false);
                continue;
            }

            // Iterate through each field definition in the Spec section.
            for (FieldDefinition specDefinition : section.getValue()) {
                validateSpecFieldDefinition(section.getKey(), specDefinition);

                // Convert Spec fieldNames to Avro friendly format if necessary.
                String avroFieldName = convertFieldNameToAvroFriendlyFormat(specDefinition.getFieldName());
                if (!avroFieldName.equals(specDefinition.getFieldName())) {
                    log.warn("Found non-Avro compatible Spec fieldName {} in section {}", specDefinition.getFieldName(),
                            section.getKey());
                    specDefinition.setFieldName(avroFieldName);
                }

                FieldDefinition recordDefinition = copyFieldDefinition(specDefinition);

                // Iterate through all the column header names checking if any match accepted names for the Spec's
                // FieldDefinition.
                boolean foundMatchingColumn = false;
                for (String columnName : columnHeaderNames) {
                    // Check if the standardized column name matches any of the standardized accepted names for this
                    // field.
                    if (doesColumnNameMatch(columnName, specDefinition.getMatchingColumnNames())) {
                        log.info("Existing field " + recordDefinition.getFieldName() + " matched column " +
                                columnName);

                        foundMatchingColumn = true;
                        matchedColumnNames.add(columnName);
                        recordDefinition.setColumnName(columnName);

                        // If there is a match and this field is a date type, autodetect the date and time formats and
                        // time zone from the column data.
                        if (UserDefinedType.DATE.equals(recordDefinition.getFieldType())) {
                            List<String> columnDataFields = resolver.getColumnFieldsByHeader(columnName);
                            MutableTriple<String, String, String> dateTimeZone = distinguishDateAndTime(
                                    columnDataFields);
                            if (dateTimeZone != null) {
                                recordDefinition.setDateFormat(dateTimeZone.getLeft());
                                recordDefinition.setTimeFormat(dateTimeZone.getMiddle());
                                recordDefinition.setTimeZone(dateTimeZone.getRight());
                            }
                        }
                        break;
                    }
                }

                recordDefinition.setInCurrentImport(foundMatchingColumn);
                // Add screen name to record FieldDefinition.
                recordDefinition.setScreenName(specDefinition.getScreenName());
                // Add the FieldDefinition to the FieldDefinitionRecord regardless of whether a match was found
                // among the column header names.
                addFieldDefinitionToRecord(recordDefinition, section.getKey(), fieldDefinitionsRecord);
            }
        }

        // Remove the set of column names matched to the Spec from the set of column names to process as custom fields.
        columnHeaderNames.removeAll(matchedColumnNames);
        String schemaInterpretationString = getSchemaInterpretationFromSpec(spec).name();
        for (String columnName : columnHeaderNames) {
            FieldDefinition recordDefinition = createNewCustomFieldDefinition(resolver, columnName,
                    schemaInterpretationString);
            addFieldDefinitionToRecord(recordDefinition, CUSTOM_FIELDS, fieldDefinitionsRecord);
        }
        return fieldDefinitionsRecord;
    }

    public static ValidateFieldDefinitionsResponse generateValidationResponse(Map<String, List<FieldDefinition>> fieldDefinitionsRecordsMap,
                                                                              Map<String, FieldDefinition> autoDetectionResultsMap,
                                                                              Map<String, List<FieldDefinition>> specFieldDefinitionsRecordsMap,
                                                                              Map<String, FieldDefinition> existingFieldDefinitionMap,
                                                                              Map<String, OtherTemplateData> otherTemplateDataMap,
                                                                              MetadataResolver resolver) {
        ValidateFieldDefinitionsResponse response = new ValidateFieldDefinitionsResponse();
        Set<String> unMappedColumnNames = fieldDefinitionsRecordsMap.getOrDefault(ImportWorkflowUtils.CUSTOM_FIELDS,
                new ArrayList<>()).stream().filter(definition -> Boolean.TRUE.equals(definition.isInCurrentImport()) &&
                StringUtils.isNotBlank(definition.getColumnName()) && !Boolean.TRUE.equals(definition.getIgnored()))
                .map(FieldDefinition::getColumnName).collect(Collectors.toSet());

        // this record the field name in existing template
        Set<String> existingFieldNameSet = new HashSet<>();
        // this info check only one user field mapped to lattice field in all section
        Set<String> mappedLatticeField = new HashSet<>();
        // generate validation message
        for (Map.Entry<String, List<FieldDefinition>> entry : fieldDefinitionsRecordsMap.entrySet()) {
            String sectionName = entry.getKey();
            List<FieldDefinition> definitions = entry.getValue();
            List<FieldValidationMessage> validations = new ArrayList<>();
            if (CUSTOM_FIELDS.equals(sectionName) || MATCH_IDS.equals(sectionName) || OTHER_IDS.equals(sectionName)) {
                // field type and date/time format for customer field, Warning
                for (FieldDefinition definition : definitions) {
                    String columnName = definition.getColumnName();
                    String fieldName = definition.getFieldName();
                    // check multiple custom field mapped to the same lattice field(template attribute)
                    checkMultipleCustomFieldMappedToLatticeField(validations, mappedLatticeField,
                            fieldName, columnName);
                    checkInExistingAndOtherTemplate(definition, existingFieldDefinitionMap, otherTemplateDataMap,
                            validations, existingFieldNameSet);
                    checkIDFields(definition, sectionName, validations);
                    if (!Boolean.TRUE.equals(definition.getIgnored()) && Boolean.TRUE.equals(definition.isInCurrentImport())) {
                        if (StringUtils.isBlank(columnName)) {
                            throw new RuntimeException("Column name %s shouldn't be empty when InCurrentImport is " +
                                    "true.");
                        }
                        FieldDefinition autoDetectedFieldDefinition =
                                autoDetectionResultsMap.get(columnName);
                        if (autoDetectedFieldDefinition == null) {
                            throw new RuntimeException(String.format("column %s doesn't exist in field definition",
                                    columnName));
                        }
                        // check type consistence
                        if (autoDetectedFieldDefinition.getFieldType() != definition.getFieldType()) {
                            String message = String.format("%s is set as %s but appears to only have %s values.",
                                    columnName, definition.getFieldType(),
                                    autoDetectedFieldDefinition.getFieldType());
                            validations.add(new FieldValidationMessage(definition.getFieldName(),
                                    columnName, message, FieldValidationMessage.MessageLevel.WARNING));
                        }
                        // check date/time format and timezone
                        if (UserDefinedType.DATE.equals(definition.getFieldType())) {
                            checkFieldDefinitionWithDateType(definition, autoDetectedFieldDefinition, resolver,
                                    validations);
                        }
                    }
                }
            } else {
                // check for lattice attribute
                List<FieldDefinition> specDefinitions = specFieldDefinitionsRecordsMap.getOrDefault(sectionName,
                        new ArrayList<>());
                Set<String> requiredFiledNames =
                        specDefinitions.stream().filter(FieldDefinition::isRequired).map(FieldDefinition::getFieldName)
                                .collect(Collectors.toSet());
                Map<String, FieldDefinition> specFieldNameToDefinition =
                        specDefinitions.stream().collect(Collectors.toMap(FieldDefinition::getFieldName,
                                field -> field));
                for (FieldDefinition definition : definitions) {
                    String fieldName = definition.getFieldName();
                    if (StringUtils.isBlank(fieldName)) {
                        throw new RuntimeException("FieldName shouldn't be empty.");
                    }
                    FieldDefinition specDefinition = specFieldNameToDefinition.getOrDefault(fieldName, null);
                    // should find definition in default spec
                    if (specDefinition == null) {
                        throw new RuntimeException("Inconsistent with lattice attribute");
                    }

                    String columnName = definition.getColumnName();
                    if (Boolean.TRUE.equals(definition.isInCurrentImport())) {
                        if (StringUtils.isBlank(columnName)) {
                            throw new RuntimeException("ColumnName shouldn't be empty when inCurrentImport is true.");
                        }
                        FieldDefinition autoDetectedFieldDefinition = autoDetectionResultsMap.get(columnName);
                        if (autoDetectedFieldDefinition == null) {
                            throw new RuntimeException(String.format("column %s doesn't exist in field definition",
                                    columnName));
                        }
                        // WARNING if the auto-detected fieldType based on column data doesn’t match the Spec defined
                        // fieldType of a Lattice Field.
                        if (autoDetectedFieldDefinition.getFieldType() != specDefinition.getFieldType()) {
                            String message = String.format("auto-detected fieldType %s based on column data %s " +
                                            "doesn’t match the Spec defined fieldType %s of a Lattice Field %s",
                                    autoDetectedFieldDefinition.getFieldType(),
                                    autoDetectedFieldDefinition.getColumnName(), specDefinition.getFieldType(),
                                    specDefinition.getFieldName());
                            validations.add(new FieldValidationMessage(fieldName,
                                    columnName, message, FieldValidationMessage.MessageLevel.WARNING));
                        }
                        if (UserDefinedType.DATE.equals(definition.getFieldType())) {
                            checkFieldDefinitionWithDateType(definition, autoDetectedFieldDefinition, resolver,
                                    validations);
                        }
                    } else {
                        // check the case user field can be mapped to lattice field
                        for (String unMappedColumnName : unMappedColumnNames) {
                            if (ImportWorkflowUtils.doesColumnNameMatch(unMappedColumnName,
                                    specDefinition.getMatchingColumnNames())) {
                                String message = String.format("Column name %s matched Lattice Field %s, but they are" +
                                        " not mapped to each other", unMappedColumnName, fieldName);
                                validations.add(new FieldValidationMessage(fieldName,
                                        columnName, message, FieldValidationMessage.MessageLevel.WARNING));
                                break;
                            }
                        }
                    }
                    // required flag check
                    if (Boolean.TRUE.equals(specDefinition.isRequired())) {
                        // if the field is required, and column name is not empty, remove fieldName from
                        // requiredFiledNames
                        if (StringUtils.isNotBlank(columnName)) {
                            requiredFiledNames.remove(fieldName);
                        }
                        if (!Boolean.TRUE.equals(definition.isRequired())) {
                            String message = String.format("Required flag is not the same for attribute %s",
                                    specDefinition.getScreenName());
                            validations.add(new FieldValidationMessage(fieldName, null, message,
                                    FieldValidationMessage.MessageLevel.ERROR));
                        }
                    }
                    // change field type for standard field , Error
                    if (!checkFieldTypeWihSpecialCase(specDefinition, definition)) {
                        String message = String.format("Data type for %s is not same to standard", specDefinition.getScreenName());
                        validations.add(new FieldValidationMessage(fieldName,
                                columnName, message, FieldValidationMessage.MessageLevel.ERROR));
                    }
                    // check multiple custom field mapped to the same lattice field(standard)
                    checkMultipleCustomFieldMappedToLatticeField(validations, mappedLatticeField, fieldName, columnName);
                    checkInExistingAndOtherTemplate(definition, existingFieldDefinitionMap, otherTemplateDataMap,
                            validations, existingFieldNameSet);
                    checkIDFields(definition, sectionName, validations);

                }

                if (CollectionUtils.isNotEmpty(requiredFiledNames)) {
                    requiredFiledNames.forEach(name -> {
                        String message = String.format("Field name %s is required, needs set column name", name);
                        validations.add(new FieldValidationMessage(name,
                                null, message, FieldValidationMessage.MessageLevel.ERROR));
                    });
                }
            }
            generateValidationForMissingDefinitionInTemplate(existingFieldDefinitionMap, existingFieldNameSet, validations);
            response.addFieldValidationMessages(entry.getKey(), validations, true);
        }

        setValidationResult(response);
        return response;
    }

    private static void setValidationResult(ValidateFieldDefinitionsResponse response) {
        if (MapUtils.isEmpty(response.getFieldValidationMessagesMap())) {
            response.setValidationResult(ValidateFieldDefinitionsResponse.ValidationResult.PASS);
        }
        boolean foundWarning = false;
        for (Map.Entry<String, List<FieldValidationMessage>> entry : response.getFieldValidationMessagesMap().entrySet()) {
            List<FieldValidationMessage> val = entry.getValue();
            for (FieldValidationMessage message : val) {
                if (FieldValidationMessage.MessageLevel.ERROR.equals(message.getMessageLevel())) {
                    response.setValidationResult(ValidateFieldDefinitionsResponse.ValidationResult.ERROR);
                    return;
                }
                if (!foundWarning && FieldValidationMessage.MessageLevel.WARNING.equals(message.getMessageLevel())) {
                    foundWarning = true;
                }
            }
        }
        if (foundWarning) {
            response.setValidationResult(ValidateFieldDefinitionsResponse.ValidationResult.WARNING);
        } else {
            response.setValidationResult(ValidateFieldDefinitionsResponse.ValidationResult.PASS);
        }
    }

    private static void checkMultipleCustomFieldMappedToLatticeField(List<FieldValidationMessage> validations,
                                                                    Set<String> mappedLatticeField,
                                                                    String fieldName, String columnName) {
        if (StringUtils.isBlank(fieldName)) {
            return ;
        }

        if (mappedLatticeField.contains(fieldName)) {
            String message = String.format("Multiple custom fields are mapped to lattice field %s",
                    fieldName);
            validations.add(new FieldValidationMessage(fieldName,
                    columnName, message, FieldValidationMessage.MessageLevel.ERROR));
        } else {
            mappedLatticeField.add(fieldName);
        }
    }

    private static void checkFieldDefinitionWithDateType(FieldDefinition definition,
                                                        FieldDefinition autoDetectedDefinition,
                                                        MetadataResolver resolver, List<FieldValidationMessage> validations) {

        // column is date type must have date format
        if (StringUtils.isBlank(definition.getDateFormat())) {
            validations.add(new FieldValidationMessage(definition.getFieldName(),
                    definition.getColumnName(), String.format("Date Format shouldn't be empty for column %s with date" +
                            " type", definition.getColumnName()), FieldValidationMessage.MessageLevel.ERROR));
            return;
        }
        String userFormat = StringUtils.isBlank(definition.getTimeFormat()) ?
                definition.getDateFormat() :
                definition.getDateFormat() + TimeStampConvertUtils.SYSTEM_DELIMITER
                        + definition.getTimeFormat();
        String formatWithBestEffort = StringUtils
                .isBlank(autoDetectedDefinition.getTimeFormat()) ?
                autoDetectedDefinition.getDateFormat() :
                autoDetectedDefinition.getDateFormat() + TimeStampConvertUtils.SYSTEM_DELIMITER
                        + autoDetectedDefinition.getTimeFormat();

        // deal with case format can't parse the value
        StringBuilder warningMessage = new StringBuilder();
        boolean match = resolver.checkUserDateType(definition.getColumnName(),
                definition.getDateFormat(), definition.getTimeFormat(), definition.getTimeZone(), warningMessage, formatWithBestEffort);
        if (!match && warningMessage.length() > 0) {
            validations.add(new FieldValidationMessage(definition.getFieldName(),
                    definition.getColumnName(), warningMessage.toString(),
                    FieldValidationMessage.MessageLevel.WARNING));
        } else if (StringUtils.isNotBlank(userFormat) && !userFormat.equals(formatWithBestEffort)) {
            // this is case that user change the date/time format which can be parsed
            String message = String.format("%s is set to %s from original %s which can parse the value from " +
                    "uploaded file.", definition.getColumnName(), userFormat, formatWithBestEffort);
            validations.add(new FieldValidationMessage(definition.getFieldName(),
                    definition.getColumnName(), message,
                    FieldValidationMessage.MessageLevel.WARNING));
        }
    }

    private static boolean checkFieldTypeWihSpecialCase(FieldDefinition specDefinition, FieldDefinition definition) {
        // A temp fix for schema update in maint_4.8.0.
        if (specDefinition.getFieldType() != definition.getFieldType()) {
            if (InterfaceName.Amount.name().equalsIgnoreCase(specDefinition.getFieldName())
                    || InterfaceName.Quantity.name().equalsIgnoreCase(specDefinition.getFieldName())
                    || InterfaceName.Cost.name().equalsIgnoreCase(specDefinition.getFieldName())) {
                if (!UserDefinedType.INTEGER.equals(definition.getFieldType())
                        && !UserDefinedType.NUMBER.equals(definition.getFieldType())) {
                    log.error(String.format("Attribute %s has wrong physicalDataType %s", definition.getFieldName(),
                            definition.getFieldType()));
                    return false;
                }
            } else if (InterfaceName.CreatedDate.name().equalsIgnoreCase(specDefinition.getFieldName())
                    || InterfaceName.LastModifiedDate.name().equalsIgnoreCase(specDefinition.getFieldName())) {
                if (!UserDefinedType.TEXT.equals(definition.getFieldType())
                        && !UserDefinedType.DATE.equals(definition.getFieldType())) {
                    log.error(String.format("Attribute %s has wrong physicalDataType %s", definition.getFieldName(),
                            definition.getFieldType()));
                    return false;
                }

            } else {
                return false;
            }
        }
        return true;
    }

    /**
     *
     * @param definition
     * @param existingFieldDefinitionMap
     * @param otherTemplateDataMap
     * @param validations
     * @param existingFieldNameSet
     * a) If no existing template and no existing other templates or batch store, allow fieldType to be set with no warning/error.
     * b) If no existing template, but other template or batch store has field, fieldType must be set to match other template and/or batch store.  If not, issue error.
     * c) If existing template and no existing other templates or batch store, allow fieldType to be changed with warning.
     * d) If existing template and other template or batch store has field, fieldType cannot be changed and must match other template and/or batch store.  If not, issue error.
     */
    private static void checkInExistingAndOtherTemplate(FieldDefinition definition,
                                                Map<String, FieldDefinition> existingFieldDefinitionMap,
                                                Map<String, OtherTemplateData> otherTemplateDataMap,
                                                List<FieldValidationMessage> validations,
                                                Set<String> existingFieldNameSet) {
        String fieldName = definition.getFieldName();
        String columnName = definition.getColumnName();
        UserDefinedType type = definition.getFieldType();
        if (MapUtils.isNotEmpty(otherTemplateDataMap)) {
            OtherTemplateData otherTemplateData = otherTemplateDataMap.get(fieldName);
            if (otherTemplateData != null) {
                UserDefinedType typeInOtherTemplate = otherTemplateData.getFieldType();
                if (type != typeInOtherTemplate && (Boolean.TRUE.equals(otherTemplateData.getInBatchStore()) ||
                        CollectionUtils.isNotEmpty(otherTemplateData.getExistingTemplateNames()))) {
                    validations.add(new FieldValidationMessage(fieldName, columnName, String.format("Field Type is not " +
                            "consistent with batch store or other template."),
                            FieldValidationMessage.MessageLevel.ERROR));
                    return;
                }
            }
        }
        if (MapUtils.isNotEmpty(existingFieldDefinitionMap) && existingFieldDefinitionMap.get(fieldName) != null) {
            // check other field to elaborate further, add validation
            FieldDefinition existingFieldDefinition = existingFieldDefinitionMap.get(fieldName);
            existingFieldNameSet.add(definition.getFieldName());
            // issue a WARNING if field type or data formats change from existing template.
            if (type != existingFieldDefinition.getFieldType()) {
                String message = String.format("the field type for existing field mapping custom Field %s -> field " +
                        "name %s will be changed to %s from %s", fieldName, columnName,
                        definition.getFieldType(), existingFieldDefinition.getFieldType());
                validations.add(new FieldValidationMessage(existingFieldDefinition.getFieldName(),
                        columnName, message,
                        FieldValidationMessage.MessageLevel.WARNING));
            }
        }
    }

    private static void generateValidationForMissingDefinitionInTemplate(Map<String, FieldDefinition> existingFieldDefinitionMap,
                                                         Set<String> existingfieldNameSet,
                                                         List<FieldValidationMessage> validations) {
        //No existing field should be removed.
        if (MapUtils.isNotEmpty(existingFieldDefinitionMap)) {
            Set<String> existingFieldNames = existingFieldDefinitionMap.keySet();
            Set<String> existingFieldNameNotInImport =
                    existingFieldNames.stream().filter(name -> !existingfieldNameSet.contains(name)).collect(Collectors.toSet());
            if (CollectionUtils.isNotEmpty(existingFieldNameNotInImport)) {
                existingFieldNameNotInImport.forEach(fieldName -> {
                    FieldDefinition existingFieldDefinition = existingFieldDefinitionMap.get(fieldName);
                    String columnName = existingFieldDefinition.getColumnName();
                    String message = String.format("existing field mapping custom Field %s -> field name %s cannot be" +
                            "removed", fieldName, columnName);
                    validations.add(new FieldValidationMessage(existingFieldDefinition.getFieldName(),
                            columnName, message,
                            FieldValidationMessage.MessageLevel.ERROR));
                });
            }
        }
    }

    private static void checkIDFields(FieldDefinition definition, String sectionName,
                                      List<FieldValidationMessage> validations) {
        if (MATCH_IDS.equals(sectionName) || OTHER_IDS.equals(sectionName) || Unique_ID.equals(sectionName)) {
            if (!UserDefinedType.TEXT.equals(definition.getFieldType())) {
                validations.add(new FieldValidationMessage(definition.getFieldName(),definition.getColumnName(),
                        String.format("Field type in %s must be Text.", sectionName),
                        FieldValidationMessage.MessageLevel.ERROR));
            }
        }
    }

}
