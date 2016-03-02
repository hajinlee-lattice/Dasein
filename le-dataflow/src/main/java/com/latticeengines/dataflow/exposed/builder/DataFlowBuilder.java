package com.latticeengines.dataflow.exposed.builder;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.FieldBuilder;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;

public abstract class DataFlowBuilder {

    private boolean local;
    private boolean checkpoint;
    private boolean enforceGlobalOrdering;
    private DataFlowContext dataFlowCtx;

    public abstract Table runFlow(DataFlowContext dataFlowCtx, String artifactVersion);

    public boolean isLocal() {
        return local;
    }

    public void setLocal(boolean local) {
        this.local = local;
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

    protected Schema createSchema(String flowName, List<FieldMetadata> fieldMetadata, DataFlowContext dataFlowCtx) {
        RecordBuilder<Schema> recordBuilder = SchemaBuilder.record("EventTable");

        if (dataFlowCtx.containsProperty("RECORDNAME")) {
            recordBuilder = SchemaBuilder.record(dataFlowCtx.getProperty("RECORDNAME", String.class));
        }

        recordBuilder = recordBuilder.prop("uuid", UUID.randomUUID().toString());
        FieldAssembler<Schema> fieldAssembler = recordBuilder.doc("Generated by " + flowName).fields();

        for (FieldMetadata fm : fieldMetadata) {
            FieldBuilder<Schema> fieldBuilder = fieldAssembler.name(fm.getFieldName());
            AbstractMap.SimpleEntry<Type, Map<String, String>> requiredProps = getRequiredProperties(fm);

            Map<String, String> props = requiredProps.getValue();

            if (dataFlowCtx != null && dataFlowCtx.getProperty("APPLYMETADATAPRUNING", Boolean.class) != null) {
                String logicalType = props.get("logicalType");
                if (logicalType != null && (logicalType.equals("id") || logicalType.equals("reference"))) {
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
            default:
                break;
            }
        }
        return fieldAssembler.endRecord();

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

    public static enum JoinType {
        INNER, //
        LEFT, //
        RIGHT, //
        OUTER
    }

    protected Table getTableMetadata(String tableName, String targetPath, List<FieldMetadata> metadata) {
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

                attribute.setLogicalDataType(fm.getPropertyValue("logicalType"));
                attribute.setNullable(true);
                attribute.setPhysicalDataType(fm.getAvroType().toString().toLowerCase());

                if (fm.getPropertyValue("precision") != null) {
                    attribute.setPrecision(Integer.parseInt(fm.getPropertyValue("precision")));
                }
                if (fm.getPropertyValue("scale") != null) {
                    attribute.setScale(Integer.parseInt(fm.getPropertyValue("scale")));
                }
                attribute.setCleanedUpEnumValuesAsString(fm.getPropertyValue("enumValues"));

                for (Map.Entry<String, String> entry : fm.getProperties().entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    if (key.equals("ApprovedUsage")) {
                        attribute.setApprovedUsage(value);
                    } else if (key.equals("StatisticalType")) {
                        attribute.setStatisticalType(value);
                    } else if (key.equals("DisplayDiscretizationStrategy")) {
                        attribute.setDisplayDiscretizationStrategy(value);
                    } else if (key.equals("Category")) {
                        attribute.setCategory(value);
                    } else if (key.equals("Tags")) {
                        attribute.setTags(Arrays.asList(StringUtils.split(value, ",")));
                    } else if (key.equals("FundamentalType")) {
                        attribute.setFundamentalType(value);
                    } else if (key.equals("RTSModuleName")) {
                        attribute.setRTSModuleName(value);
                    } else if (key.equals("RTSArguments")) {
                        attribute.setRTSArguments(value);
                    } else if (key.equals("RTSAttribute")) {
                        attribute.setRTS(Boolean.valueOf(value));
                    }
                }

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

    public static class Aggregation {
        public static enum AggregationType {
            MAX, //
            MIN, //
            SUM(new FieldMetadata(Type.DOUBLE, Double.class, null, null)), //
            COUNT(new FieldMetadata(Type.LONG, Long.class, null, null)), //
            AVG(new FieldMetadata(Type.DOUBLE, Double.class, null, null)), //
            FIRST, //
            LAST;

            private FieldMetadata fieldMetadata;

            AggregationType() {
                this(null);
            }

            AggregationType(FieldMetadata fieldMetadata) {
                this.fieldMetadata = fieldMetadata;
            }

            public FieldMetadata getFieldMetadata() {
                return fieldMetadata;
            }

        }

        private final String aggregatedFieldName;
        private final String targetFieldName;
        private final AggregationType aggregationType;
        private final FieldList outputFieldStrategy;

        public Aggregation(String aggregatedFieldName, String targetFieldName, AggregationType aggregationType) {
            this(aggregatedFieldName, targetFieldName, aggregationType, null);
        }

        public Aggregation(AggregationType aggregationType, FieldList outputFieldStrategy) {
            this(null, null, aggregationType, outputFieldStrategy);
        }

        public Aggregation(String aggregatedFieldName, String targetFieldName, AggregationType aggregationType,
                FieldList outputFieldStrategy) {
            this.aggregatedFieldName = aggregatedFieldName;
            this.targetFieldName = targetFieldName;
            this.aggregationType = aggregationType;
            this.outputFieldStrategy = outputFieldStrategy;
        }

        public String getAggregatedFieldName() {
            return aggregatedFieldName;
        }

        public String getTargetFieldName() {
            return targetFieldName;
        }

        public AggregationType getAggregationType() {
            return aggregationType;
        }

        public FieldList getOutputFieldStrategy() {
            return outputFieldStrategy;
        }
    }

    public static class FieldMetadata {
        private Schema.Type avroType;
        private Class<?> javaType;
        private String fieldName;
        private Field avroField;
        private Map<String, String> properties = new HashMap<>();

        public FieldMetadata(FieldMetadata fm) {
            this(fm.getAvroType(), fm.javaType, fm.getFieldName(), fm.getField(), fm.getProperties());
        }

        public FieldMetadata(String fieldName, Class<?> javaType) {
            this(AvroUtils.getAvroType(javaType), javaType, fieldName, null);
        }

        public FieldMetadata(String fieldName, Class<?> javaType, Map<String, String> properties) {
            this(AvroUtils.getAvroType(javaType), javaType, fieldName, null);
            properties.putAll(properties);
        }

        @SuppressWarnings("deprecation")
        public FieldMetadata(Schema.Type avroType, Class<?> javaType, String fieldName, Field avroField) {
            this.avroType = avroType;
            this.javaType = javaType;
            this.fieldName = fieldName;
            this.avroField = avroField;

            if (avroField != null) {
                properties.putAll(avroField.props());
            }
        }

        public FieldMetadata(Schema.Type avroType, Class<?> javaType, String fieldName, Field avroField,
                Map<String, String> properties) {
            this(avroType, javaType, fieldName, avroField);
            if (avroField == null && properties != null) {
                this.properties.putAll(properties);
            }
        }

        public Schema.Type getAvroType() {
            return avroType;
        }

        public Class<?> getJavaType() {
            return javaType;
        }

        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public Field getField() {
            return avroField;
        }

        public String getPropertyValue(String key) {
            return properties.get(key);
        }

        public void setPropertyValue(String key, String value) {
            properties.put(key, value);
        }

        public Set<Entry<String, String>> getEntries() {
            return properties.entrySet();
        }

        public Map<String, String> getProperties() {
            return properties;
        }

        @Override
        public String toString() {
            return fieldName;
        }
    }

    public static class FieldList {
        public static FieldList ALL = new FieldList(KindOfFields.ALL);
        public static FieldList RESULTS = new FieldList(KindOfFields.RESULTS);
        public static FieldList GROUP = new FieldList(KindOfFields.GROUP);
        public static FieldList NONE = new FieldList(KindOfFields.NONE);

        public enum KindOfFields {
            ALL, //
            RESULTS, //
            GROUP, //
            NONE
        }

        private List<String> fields;
        private KindOfFields kind;

        public FieldList(KindOfFields kind) {
            this.kind = kind;
        }

        public FieldList(String... fields) {
            this.fields = Arrays.asList(fields);
        }

        public FieldList(List<String> fields) {
            this.fields = new ArrayList<>(fields);
        }

        public String[] getFields() {
            if (fields == null) {
                return null;
            }
            return fields.toArray(new String[fields.size()]);
        }

        public List<String> getFieldsAsList() {
            if (fields == null) {
                return null;
            }
            return new ArrayList<>(fields);
        }

        public FieldList addAll(List<String> fields) {
            FieldList toReturn = new FieldList(this.fields);
            toReturn.kind = this.kind;
            toReturn.fields.addAll(fields);
            return toReturn;
        }

        public KindOfFields getKind() {
            return kind;
        }

    }

}
