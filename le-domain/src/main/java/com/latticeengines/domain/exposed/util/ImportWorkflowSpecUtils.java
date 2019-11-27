package com.latticeengines.domain.exposed.util;

import static com.latticeengines.common.exposed.util.AvroUtils.getAvroFriendlyString;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.AttributeBuilder;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinition;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.query.EntityType;

public class ImportWorkflowSpecUtils {
    private static final Logger log = LoggerFactory.getLogger(ImportWorkflowSpecUtils.class);

    protected static final String AVRO_FIELD_NAME_PREFIX = "avro_";
    protected static final String USER_PREFIX = "user_";

    // Generate a table from a FieldDefinitionsRecord.  The writeAllDefinitions flag defines whether FieldDefinitions
    // that haven't ever matched an import file column should be written back.  The typical usage of this flag is to
    // set it true when converting a Spec to a Table, since in that case all Spec fields should be written to the
    // Table.  It should be set false when converting a FieldDefinitionsRecord to a Table that will be written to
    // DataFeedTask template because Spec fields that were never matched to input columns should not be in the
    // Metadata Attributes table.
    public static Table getTableFromFieldDefinitionsRecord(
            String tableName, boolean writeAllDefinitions, FieldDefinitionsRecord record) {
        Table table = new Table();
        if (StringUtils.isBlank(record.getSystemObject())) {
            throw new IllegalArgumentException(
                    "FieldDefinitionRecord must have SystemObject defined to create a table from it");
        }
        String schemaInterpretationString = EntityType.fromDisplayNameToEntityType(record.getSystemObject())
                .getSchemaInterpretation().toString();
        table.setInterpretation(schemaInterpretationString);
        table.setName(StringUtils.isNotBlank(tableName) ? tableName : schemaInterpretationString);
        table.setDisplayName(schemaInterpretationString);

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
            attrName = getAvroFriendlyString(definition.getColumnName());
            attrName = USER_PREFIX + attrName;
        } else {
            attrName = getAvroFriendlyString(definition.getFieldName());
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

}
