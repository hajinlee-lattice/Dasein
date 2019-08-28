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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.commons.text.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinition;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.util.AttributeUtils;
import com.latticeengines.pls.metadata.resolution.MetadataResolver;

public class ImportWorkflowUtils {
    private static final Logger log = LoggerFactory.getLogger(ImportWorkflowUtils.class);

    protected static final String USER_PREFIX = "user_";
    // TODO(jwinter): Reconsider if the Spec section for Custom Fields should be indicated in a different manner
    //     rather than hard coded.
    // String representing the section of the template reserved for non-standard customer generated fields.
    private static final String CUSTOM_FIELDS = "Custom Fields";

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
            String systemName, String systemType, String systemObject, String importFile, String requestType)
            throws LedpException {
        log.info("Field Definition Request Parameters:\n   systemName: " + systemName + "\n   systemType: " +
                 systemType + "\n   systemObject: " + systemObject + "\n   importFile: " + importFile);

        // TODO(jwinter): Figure out what validation is needed.

        if (StringUtils.isBlank(systemName)) {
            log.error("systemName is null or blank");
            throw new LedpException(LedpCode.LEDP_18229, new String[] { requestType, "systemName" });
        }

        if (StringUtils.isBlank(systemType)) {
            log.error("systemType is null or blank");
            throw new LedpException(LedpCode.LEDP_18229, new String[] { requestType, "systemType" });
        }

        if (StringUtils.isBlank(systemObject)) {
            log.error("systemObject is null or blank");
            throw new LedpException(LedpCode.LEDP_18229, new String[] { requestType, "systemObject" });
        }

        // Make sure systemObject maps to EntityType displayName.
        try {
            EntityType.fromDisplayNameToEntityType(systemObject);
        } catch (IllegalArgumentException e) {
            log.error("systemObject is not valid EntityType displayName");
            throw new LedpException(LedpCode.LEDP_18229, new String[] { requestType, "systemObject" });
        }

        if (StringUtils.isBlank(importFile)) {
            log.error("importFile is null or blank");
            throw new LedpException(LedpCode.LEDP_18229, new String[] { requestType, "importFile" });
        }
    }

    public static void validateFieldDefinitionRecord(FieldDefinitionsRecord record, String requestType) {
        // Make sure that the commit request has field definition records section.
        if (record == null || MapUtils.isEmpty(record.getFieldDefinitionsRecordsMap())) {
            log.error("FieldDefinitionsRecord is null or missing FieldDefinitions map");
            throw new LedpException(LedpCode.LEDP_18229, new String[]{requestType, "FieldDefintionsRecord"});
        }
    }

    // TODO(jwinter): This is copied from test code and likely a hack.  Find a better way of doing this.
    public static String getFeedTypeFromSystemNameAndEntityType(String system, EntityType entityType) {
        return system + SPLIT_CHART + entityType.getDefaultFeedTypeName();
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

    private static FieldDefinition copyDefinitionFromSpec(FieldDefinition specDefinition) {
        FieldDefinition definition = new FieldDefinition();
        definition.setFieldName(specDefinition.getFieldName());
        definition.setFieldType(specDefinition.getFieldType());
        definition.setRequired(specDefinition.isRequired());
        definition.setApprovedUsage(specDefinition.getApprovedUsage());
        definition.setLogicalDataType(specDefinition.getLogicalDataType());
        definition.setFundamentalType(specDefinition.getFundamentalType());
        definition.setStatisticalType(specDefinition.getStatisticalType());
        definition.setCategory(specDefinition.getCategory());
        definition.setSubcategory(specDefinition.getSubcategory());
        return definition;
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
        for (Map.Entry<String, List<FieldDefinition>> section : spec.getFieldDefinitionsRecordsMap().entrySet()) {
            if (CollectionUtils.isEmpty(section.getValue())) {
                fieldDefinitionsRecord.addFieldDefinitionsRecords(section.getKey(), new ArrayList<>(), false);
                continue;
            }

            // Iterate through each field definition in the Spec section.
            for (FieldDefinition specDefinition : section.getValue()) {
                if (specDefinition == null) {
                    log.error("During spec iteration, found null FieldDefinition in section " + section.getKey());
                    throw new IllegalArgumentException("During spec iteration, found null FieldDefinition in section " +
                            section.getKey());
                } else if (StringUtils.isBlank(specDefinition.getFieldName())) {
                    log.error("During spec iteration, found FieldDefinition with null fieldName in section "
                            + section.getKey());
                    throw new IllegalArgumentException("During spec iteration, found FieldDefinition with null fieldName in section "
                            + section.getKey());
                } else if (CollectionUtils.isEmpty(specDefinition.getMatchingColumnNames())) {
                    log.error("During spec iteration, found FieldDefinition with null/empty matchingColumnNames in section "
                            + section.getKey());
                    throw new IllegalArgumentException("During spec iteration, found FieldDefinition with null/empty matchingColumnNames in section "
                            + section.getKey());
                }

                log.info("Spec section: " + section.getKey() + "  fieldName: " + specDefinition.getFieldName());
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
                    recordDefinition = copyDefinitionFromSpec(specDefinition);

                    // Iterate through all the column header names checking if any match the set of accepted names for
                    // the Spec's FieldDefinition.
                    boolean foundMatchingColumn = false;
                    Iterator<String> columnIterator = columnHeaderNames.iterator();
                    while (columnIterator.hasNext()) {
                        String columnName = columnIterator.next();
                        // Check if the standardized column name matches any of the standardized accepted names for this
                        // field.
                        if (doesColumnNameMatch(columnName, specDefinition)) {
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
            // TODO(jwinter): Should we be setting ScreenName for Custom Fields?
            //existingDefinition.setScreenName(existingDefinition.getColumnName());
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

    private static boolean doesColumnNameMatch(String columnName, FieldDefinition recordDefinition){
        List<String> matchingColumnNames = recordDefinition.getMatchingColumnNames();
        if (CollectionUtils.isNotEmpty(matchingColumnNames)) {
            final String standardizedColumnName = MetadataResolver.standardizeAttrName(columnName);
            String matchedColumnName = matchingColumnNames.stream() //
                    .filter(allowedName -> MetadataResolver.standardizeAttrName(allowedName)
                            .equalsIgnoreCase(standardizedColumnName)) //
                    .findFirst().orElse(null);
            return StringUtils.isNotBlank(matchedColumnName);
        }
        return false;
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

    private static void addFieldDefinitionToRecord(FieldDefinition definition, String section,
                                                   FieldDefinitionsRecord record) {
        if (!record.addFieldDefinition(section, definition, false)) {
            log.error("Could not add FieldDefinition with fieldName " + definition.getFieldName() +
                    " to section " + section + " because of existing record");
            throw new IllegalArgumentException("Could not add FieldDefinition with fieldName " +
                    definition.getFieldName()  + " to section " + section + " because of existing record");
        }
        log.info("    Successfully added Spec fieldName " + definition.getFieldName() + " to section " + section);
    }

    private static FieldDefinition createNewCustomFieldDefinition(MetadataResolver resolver, String columnName,
                                                                  String schemaInterpretationString) {
        FieldDefinition definition = new FieldDefinition();
        // columnName must be set before fieldType.
        definition.setColumnName(columnName);
        definition.setFieldType(getFieldTypeFromColumnContent(definition, resolver));
        String escapedColumnName = StringEscapeUtils.escapeHtml4(columnName);
        if (!columnName.equals(escapedColumnName)) {
            definition.setColumnName(escapedColumnName);
        }
        // TODO(jwinter): Should we be setting ScreenName for Custom Fields?
        //recordDefinition.setScreenName(columnName);
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
                // do not current or did not previously match a import file column.
                if (writeAllDefinitions || StringUtils.isNotBlank(definition.getColumnName())) {
                    Attribute attribute = getAttributeFromFieldDefinition(definition);
                    table.addAttribute(attribute);
                    log.info("   SectionName: " + entry.getKey() + " FieldName: " + definition.getFieldName());
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
                if (specDefinition == null) {
                    log.error("During spec iteration, found null FieldDefinition in section " + section.getKey());
                    throw new IllegalArgumentException("During spec iteration, found null FieldDefinition in section " +
                            section.getKey());
                } else if (StringUtils.isBlank(specDefinition.getFieldName())) {
                    log.error("During spec iteration, found FieldDefinition with null fieldName in section "
                            + section.getKey());
                    throw new IllegalArgumentException("During spec iteration, found FieldDefinition with null fieldName in section "
                            + section.getKey());
                } else if (CollectionUtils.isEmpty(specDefinition.getMatchingColumnNames())) {
                    log.error("During spec iteration, found FieldDefinition with null/empty matchingColumnNames in section "
                            + section.getKey());
                    throw new IllegalArgumentException("During spec iteration, found FieldDefinition with null/empty matchingColumnNames in section "
                            + section.getKey());
                }

                log.info("Spec section: " + section.getKey() + "  fieldName: " + specDefinition.getFieldName());
                // Convert Spec fieldNames to Avro friendly format if necessary.
                String avroFieldName = convertFieldNameToAvroFriendlyFormat(specDefinition.getFieldName());
                if (!avroFieldName.equals(specDefinition.getFieldName())) {
                    log.warn("Found non-Avro compatible Spec fieldName {} in section {}", specDefinition.getFieldName(),
                            section.getKey());
                    specDefinition.setFieldName(avroFieldName);
                }

                FieldDefinition recordDefinition = copyDefinitionFromSpec(specDefinition);

                // Iterate through all the column header names checking if any match accepted names for the Spec's
                // FieldDefinition.
                boolean foundMatchingColumn = false;
                for (String columnName : columnHeaderNames) {
                    // Check if the standardized column name matches any of the standardized accepted names for this
                    // field.
                    if (doesColumnNameMatch(columnName, specDefinition)) {
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

    // OLD CODE, KEEP AROUND UNTIL CONFIRMED TO BE UNNEEDED
    // Requires Attribute class to have property Spec Section Name to work.
    /*
    public static FieldDefinitionsRecord createFieldDefinitionsRecordFromSpecAndTableOld(
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
        Map<String, FieldDefinition> fieldNameToDefinitionMap = new HashMap<>();

        if (existingTable != null) {
            for (Attribute attribute : existingTable.getAttributes()) {
                FieldDefinition fieldDefinition = new FieldDefinition();
                fieldDefinition.setFieldName(attribute.getName());
                fieldDefinition.setFieldType(MetadataResolver.getFieldTypeFromPhysicalType(
                        attribute.getPhysicalDataType()));
                fieldDefinition.setColumnName(attribute.getDisplayName());
                fieldDefinition.setRequired(attribute.getRequired());
                fieldDefinition.setInCurrentImport(columnHeaderNames.contains(attribute.getDisplayName()));
                fieldDefinition.setDateFormat(attribute.getDateFormatString());
                fieldDefinition.setTimeFormat(attribute.getTimeFormatString());
                fieldDefinition.setTimeZone(attribute.getTimezone());

                // Remove column name from set of column header names eligible to match to new fields in the Spec.
                columnHeaderNames.remove(fieldDefinition.getColumnName());

                // Store FieldDefinitions in a easy to look up data structure, keyed by FieldName.
                fieldNameToDefinitionMap.put(fieldDefinition.getFieldName(), fieldDefinition);
                log.info("Adding fieldName " + fieldDefinition.getFieldName() + " to existing field map");
                // Put the existing FieldDefinitions in the merged record.
                if (!fieldDefinitionsRecord.addFieldDefinition(attribute.getSpecSectionName(), fieldDefinition,
                        false)) {
                    log.info("Could not add FieldDefinition with fieldName " + fieldDefinition.getFieldName() +
                            " to section " + attribute.getSpecSectionName() + " because of existing record");
                    throw new IllegalArgumentException(
                            "Could not add FieldDefinition with fieldName " + fieldDefinition.getFieldName()  +
                                    " to section " + attribute.getSpecSectionName() + " because of existing record");
                }
                log.info("Successfully added Existing fieldName " + fieldDefinition.getFieldName() + " to section " +
                        attribute.getSpecSectionName());
            }
        }

        // Track all the column header names that match with field definitions from the Spec.
        Set<String> matchedColumnNames = new HashSet<>();

        // Iteration through all the sections in the Spec.
        for (Map.Entry<String, List<FieldDefinition>> section : spec.getFieldDefinitionsRecordsMap().entrySet()) {
            if (CollectionUtils.isEmpty(section.getValue())) {
                continue;
            }

            // Iterate through each field definition in the Spec section.
            for (FieldDefinition specDefinition : section.getValue()) {
                if (specDefinition == null) {
                    log.error("During spec iteration, found null FieldDefinition in section " + section.getKey());
                    continue;
                } else if (StringUtils.isBlank(specDefinition.getFieldName())) {
                    log.error("During spec iteration, found FieldDefinition with null fieldName in section "
                            + section.getKey());
                    continue;
                } else if (CollectionUtils.isEmpty(specDefinition.getMatchingColumnNames())) {
                    log.error("During spec iteration, found FieldDefinition with null/empty matchingColumnNames in section "
                            + section.getKey());
                    continue;
                }

                log.info("Spec section: " + section.getKey() + "  fieldName: " + specDefinition.getFieldName());
                if (fieldNameToDefinitionMap.containsKey(specDefinition.getFieldName())) {
                    // If the existing template already has a mapping for this field, only the displayName needs to
                    // be set.
                    log.info("Found fieldName " + specDefinition.getFieldName() + " in existing field defintions map");
                    fieldNameToDefinitionMap.get(specDefinition.getFieldName()).setScreenName(
                            specDefinition.getScreenName());
                    continue;
                }

                FieldDefinition recordDefinition = new FieldDefinition();
                recordDefinition.setFieldName(specDefinition.getFieldName());
                recordDefinition.setFieldType(specDefinition.getFieldType());
                recordDefinition.setScreenName(specDefinition.getScreenName());
                recordDefinition.setRequired(specDefinition.isRequired());

                // Iterate through all the column header names checking if any match accepted names for the Spec's
                // FieldDefinition.
                boolean foundMatchingColumn = false;
                for (String columnName : columnHeaderNames) {
                    // Check if the standardized column name matches any of the standardized accepted names for this
                    // field.
                    if (doesColumnNameMatch(columnName, specDefinition)) {
                        foundMatchingColumn = true;
                        columnHeaderNames.remove(columnName);
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

                // Add the record definition to the FieldDefinitionRecord regardless of whether a match was found
                // among the column header names.
                if (!fieldDefinitionsRecord.addFieldDefinition(section.getKey(), recordDefinition,
                        false)) {
                    log.error("Could not add FieldDefinition with fieldName " + recordDefinition.getFieldName() +
                            " to section " + section.getKey() + " because of existing record");
                    throw new IllegalArgumentException(
                            "Could not add FieldDefinition with fieldName " + recordDefinition.getFieldName()  +
                                    " to section " + section.getKey() + " because of existing record");
                }
                log.info("Successfully added Spec fieldName " + recordDefinition.getFieldName() + " to section " +
                        section.getKey());
            }
        }

        // A FieldDefinition should be created for the remaining column header names which should be added as custom
        // fields.
        for (String columnName : columnHeaderNames) {
            FieldDefinition recordDefinition = new FieldDefinition();
            recordDefinition.setColumnName(StringEscapeUtils.escapeHtml4(columnName));
            recordDefinition.setFieldType(getFieldTypeFromColumnContent(recordDefinition, resolver));
            // TODO(jwinter): Should we be setting ScreenName for Custom Fields?
            recordDefinition.setScreenName(columnName);
            recordDefinition.setRequired(false);
            recordDefinition.setInCurrentImport(true);
            if (!fieldDefinitionsRecord.addFieldDefinition(CUSTOM_FIELDS, recordDefinition,
                    false)) {
                log.error("Could not add FieldDefinition with columnName " + recordDefinition.getColumnName() +
                        " to section " + CUSTOM_FIELDS + " because of existing record");
                throw new IllegalArgumentException(
                        "Could not add FieldDefinition with columnName " + recordDefinition.getColumnName()  +
                                " to section " + CUSTOM_FIELDS + " because of existing record");
            }
            log.info("Successfully added Custom columnName " + recordDefinition.getColumnName() + " to section " +
                    CUSTOM_FIELDS);

        }
        return fieldDefinitionsRecord;
    }
    */

    // OLD CODE, KEEP AROUND UNTIL CONFIRMED TO BE UNNEEDED
    // Requires Attribute class to have property Spec Section Name to work.
    /*
    public static FieldDefinitionsRecord createFieldDefinitionsRecordFromTable(Table table, MetadataResolver resolver) {
        FieldDefinitionsRecord fieldDefinitionsRecord = new FieldDefinitionsRecord();
        if (table == null) {
            return fieldDefinitionsRecord;
        }

        // Get column names from imported file.
        Set<String> columnNames = resolver.getHeaderFields();
        Set<String> matchedColumnNames = new HashSet<>();

        for (Attribute attribute : table.getAttributes()) {
            FieldDefinition fieldDefinition = new FieldDefinition();
            fieldDefinition.setFieldName(attribute.getName());
            fieldDefinition.setFieldType(MetadataResolver.getFieldTypeFromPhysicalType(
                    attribute.getPhysicalDataType()));
            //fieldDefinition.setScreenName();
            fieldDefinition.setColumnName(attribute.getDisplayName());
            fieldDefinition.setRequired(attribute.getRequired());
            //fieldDefinition.setInCurrentImport();
            fieldDefinition.setDateFormat(attribute.getDateFormatString());
            fieldDefinition.setTimeFormat(attribute.getTimeFormatString());
            fieldDefinition.setTimeZone(attribute.getTimezone());

            if (!fieldDefinitionsRecord.addFieldDefinition(attribute.getSpecSectionName(), fieldDefinition,
                    false)) {
                log.error("Could not add FieldDefinition with fieldName " + fieldDefinition.getFieldName() +
                        " to section " + attribute.getSpecSectionName() + " because of existing record");
                throw new IllegalArgumentException(
                        "Could not add FieldDefinition with fieldName " + fieldDefinition.getFieldName()  +
                                " to section " + attribute.getSpecSectionName() + " because of existing record");
            }
        }
        return fieldDefinitionsRecord;
    }
    */

}
