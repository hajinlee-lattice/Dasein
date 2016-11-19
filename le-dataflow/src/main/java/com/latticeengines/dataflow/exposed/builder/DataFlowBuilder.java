package com.latticeengines.dataflow.exposed.builder;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.FieldBuilder;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.AttributeUtils;

public abstract class DataFlowBuilder {
    private static final Log log = LogFactory.getLog(DataFlowBuilder.class);

    private boolean local;
    private boolean checkpoint;
    private boolean enforceGlobalOrdering;
    private boolean debug;
    private DataFlowContext dataFlowCtx;

    public abstract Table runFlow(DataFlowContext dataFlowCtx, String artifactVersion);

    public boolean isLocal() {
        return local;
    }

    public void setLocal(boolean local) {
        this.local = local;
    }

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    @SuppressWarnings("deprecation")
    private AbstractMap.SimpleEntry<Type, Map<String, String>> getRequiredProperties(FieldMetadata fm) {
        Map<String, String> map = null;
        Field avroField = fm.getField();

        if (avroField != null) {
            map = avroField.props();
        } else {
            map = fm.getProperties();
        }
        return new AbstractMap.SimpleEntry<>(fm.getAvroType(), map);
    }

    public boolean isCheckpoint() {
        return checkpoint;
    }

    public void setCheckpoint(boolean checkpoint) {
        this.checkpoint = checkpoint;
    }

    public boolean enforceGlobalOrdering() {
        return enforceGlobalOrdering;
    }

    public void setEnforceGlobalOrdering(boolean enforceGlobalOrdering) {
        this.enforceGlobalOrdering = enforceGlobalOrdering;
    }

    public DataFlowContext getDataFlowCtx() {
        return dataFlowCtx;
    }

    public void setDataFlowCtx(DataFlowContext dataFlowCtx) {
        this.dataFlowCtx = dataFlowCtx;
    }

    protected Schema createSchema(String flowName, List<FieldMetadata> fieldMetadata, DataFlowContext dataFlowCtx) {
        RecordBuilder<Schema> recordBuilder = SchemaBuilder.record("EventTable");

        if (dataFlowCtx.containsProperty(DataFlowProperty.RECORDNAME)) {
            recordBuilder = SchemaBuilder.record(dataFlowCtx.getProperty(DataFlowProperty.RECORDNAME, String.class));
        }

        recordBuilder = recordBuilder.prop("uuid", UUID.randomUUID().toString());
        FieldAssembler<Schema> fieldAssembler = recordBuilder.doc("Generated by " + flowName).fields();

        for (FieldMetadata fm : fieldMetadata) {
            FieldBuilder<Schema> fieldBuilder = fieldAssembler.name(fm.getFieldName());
            AbstractMap.SimpleEntry<Type, Map<String, String>> requiredProps = getRequiredProperties(fm);

            Map<String, String> props = requiredProps.getValue();

            if (dataFlowCtx != null
                    && dataFlowCtx.getProperty(DataFlowProperty.APPLYMETADATAPRUNING, Boolean.class) != null) {
                String logicalType = props.get("logicalType");
                if (logicalType != null && //
                        (logicalType.equals(LogicalDataType.InternalId.toString()) || //
                                logicalType.equals(LogicalDataType.Reference.toString()) || //
                        logicalType.equals(LogicalDataType.Id))) {
                    continue;
                }
            }
            for (Map.Entry<String, String> entry : props.entrySet()) {
                String k = entry.getKey();
                String v = entry.getValue();

                if (k.equals("uuid")) {
                    continue;
                }
                fieldBuilder = fieldBuilder.prop(k, v);
            }

            fieldBuilder.prop("uuid", UUID.randomUUID().toString());

            Type type = requiredProps.getKey();

            switch (type) {
            case DOUBLE:
                fieldAssembler = fieldBuilder.type().unionOf().doubleType().and().nullType().endUnion().noDefault();
                break;
            case FLOAT:
                fieldAssembler = fieldBuilder.type().unionOf().floatType().and().nullType().endUnion().noDefault();
                break;
            case INT:
                fieldAssembler = fieldBuilder.type().unionOf().intType().and().nullType().endUnion().noDefault();
                break;
            case LONG:
                fieldAssembler = fieldBuilder.type().unionOf().longType().and().nullType().endUnion().noDefault();
                break;
            case STRING:
                fieldAssembler = fieldBuilder.type().unionOf().stringType().and().nullType().endUnion().noDefault();
                break;
            case BOOLEAN:
                fieldAssembler = fieldBuilder.type().unionOf().booleanType().and().nullType().endUnion().noDefault();
                break;
            case ARRAY:
                fieldAssembler = fieldBuilder.type().array().items(fm.getListElementSchema()).noDefault();
            default:
                break;
            }
        }
        return fieldAssembler.endRecord();

    }

    // Conversion to FieldMetadata
    protected List<FieldMetadata> getFieldMetadata(Map<String, Field> fieldMap, Table table) {
        List<FieldMetadata> fields = new ArrayList<>(fieldMap.size());

        for (Field field : fieldMap.values()) {
            Type avroType = Type.NULL;
            for (Schema schema : field.schema().getTypes()) {
                avroType = schema.getType();
                if (!Type.NULL.equals(avroType)) {
                    break;
                }
            }
            FieldMetadata fm = new FieldMetadata(avroType, AvroUtils.getJavaType(avroType), field.name(), field);
            fm.setTableName(table.getName());

            // Merge in properties from Attribute on the Table
            Attribute attribute = table.getAttribute(field.name());
            if (attribute != null) {
                if (fm.getPropertyValue("displayName") == null) {
                    fm.setPropertyValue("displayName", attribute.getDisplayName());
                }
                if (fm.getPropertyValue("length") == null) {
                    fm.setPropertyValue("length", attribute.getLength() != null ? attribute.getLength().toString()
                            : null);
                }
                if (fm.getPropertyValue("sourceLogicalType") == null) {
                    fm.setPropertyValue("sourceLogicalType", attribute.getSourceLogicalDataType());
                }
                if (fm.getPropertyValue("logicalType") == null) {
                    fm.setPropertyValue("logicalType", attribute.getLogicalDataType() != null ? attribute
                            .getLogicalDataType().toString() : null);
                }
                if (fm.getPropertyValue("precision") != null) {
                    fm.setPropertyValue("precision", attribute.getPrecision() != null ? attribute.getPrecision()
                            .toString() : null);
                }
                if (fm.getPropertyValue("scale") != null) {
                    fm.setPropertyValue("scale", attribute.getScale() != null ? attribute.getScale().toString() : null);
                }
                if (fm.getPropertyValue("enumValues") != null) {
                    fm.setPropertyValue("enumValues", attribute.getCleanedUpEnumValuesAsString());
                }
                fm.setNullable(attribute.getNullable());

                Map<String, Object> properties = attribute.getProperties();
                for (String key : properties.keySet()) {
                    Object metadataValue = properties.get(key);
                    String avroValue = fm.getPropertyValue(key);
                    if (avroValue != null && metadataValue != null && !avroValue.equals(metadataValue.toString())) {
                        log.warn(String
                                .format("Property collision for field %s in table %s.  Value is %s in avro but %s in metadata table.  Using metadataValue from metadata table",
                                        field.name(), table.getName(), avroValue, metadataValue));
                    }
                    fm.setPropertyValue(key, metadataValue != null ? metadataValue.toString() : null);
                }
            } else {
                log.warn(String.format(
                        "Could not find field %s in table %s.  Attribute is in avro but not in metadata", field.name(),
                        table.getName()));
            }

            fields.add(fm);
        }
        return fields;
    }

    // Conversion to Table
    protected Table getTableMetadata(String tableName, String targetPath, List<FieldMetadata> metadata) {
        Boolean cascade = getDataFlowCtx().getProperty(DataFlowProperty.CASCADEMETADATA, Boolean.class, false);
        if (cascade) {
            cascadeMetadata(metadata);
        }

        Table table = new Table();
        table.setName(tableName);
        table.setDisplayName(tableName);

        for (FieldMetadata fm : metadata) {
            try {
                Attribute attribute = new Attribute();
                attribute.setName(fm.getFieldName());
                String displayName = fm.getPropertyValue("displayName") != null ? fm.getPropertyValue("displayName")
                        : fm.getFieldName();
                attribute.setDisplayName(displayName);
                if (fm.getPropertyValue("length") != null) {
                    attribute.setLength(Integer.parseInt(fm.getPropertyValue("length")));
                }

                attribute.setSourceLogicalDataType(fm.getPropertyValue("sourceLogicalType"));
                attribute.setLogicalDataType(fm.getPropertyValue("logicalType"));
                attribute.setNullable(fm.isNullable());

                attribute.setPhysicalDataType(fm.getAvroType().toString().toLowerCase());

                if (fm.getPropertyValue("precision") != null) {
                    attribute.setPrecision(Integer.parseInt(fm.getPropertyValue("precision")));
                }
                if (fm.getPropertyValue("scale") != null) {
                    attribute.setScale(Integer.parseInt(fm.getPropertyValue("scale")));
                }
                attribute.setCleanedUpEnumValuesAsString(fm.getPropertyValue("enumValues"));

                AttributeUtils.setPropertiesFromStrings(attribute, fm.getProperties());

                table.addAttribute(attribute);
            } catch (Exception e) {
                throw new RuntimeException(String.format("Failed to convert field %s to output metadata format",
                        fm.getFieldName()), e);
            }
        }

        Extract extract = new Extract();
        extract.setName("extract");
        extract.setPath(targetPath);
        extract.setExtractionTimestamp(DateTime.now().getMillis());
        table.addExtract(extract);

        return table;
    }

    private void cascadeMetadata(List<FieldMetadata> metadata) {
        MetadataCascade cascade = new MetadataCascade(metadata);
        cascade.cascade();
    }

}
