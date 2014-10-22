package com.latticeengines.dataflow.exposed.builder;

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
import org.apache.commons.math3.util.Pair;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

public abstract class DataFlowBuilder {

    private boolean local;

    public abstract String constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources);

    public abstract Schema getSchema(String flowName, String operatorName);

    public abstract void runFlow(DataFlowContext dataFlowCtx);

    protected abstract void addSource(String sourceName, String sourcePath);

    protected abstract String addInnerJoin(String lhs, FieldList lhsJoinFields, String rhs, FieldList rhsJoinFields);

    protected abstract String addInnerJoin(List<JoinCriteria> joinCriteria);

    protected abstract String addGroupBy(String prior, FieldList groupByFields, List<GroupByCriteria> groupByCriteria);

    protected abstract String addGroupBy(String prior, FieldList groupByFieldList, FieldList sortFieldList,
            List<GroupByCriteria> groupByCriteria);

    protected abstract String addFilter(String prior, String expression, FieldList filterFields);

    protected abstract String addFunction(String prior, String expression, FieldList fieldsToApply, FieldMetadata fieldToCreate);
    
    protected abstract String addRowId(String prior, String fieldName, String tableName);
    
    protected abstract String addMD5(String prior, FieldList fieldsToApply, String targetFieldName);
    
    protected abstract List<FieldMetadata> getMetadata(String operator);

    public DataFlowBuilder() {
        this(false);
    }

    public DataFlowBuilder(boolean local) {
        this.local = local;
    }

    public boolean isLocal() {
        return local;
    }

    public void setLocal(boolean local) {
        this.local = local;
    }
    
    @SuppressWarnings("deprecation")
    private Pair<Type, Map<String, String>> getRequiredProperties(FieldMetadata fm) {
        Map<String, String> map = null;
        Field avroField = fm.getField();
        
        if (avroField != null) {
            map = avroField.props();
        } else {
            map = fm.getProperties();
        }
        return new Pair<>(fm.getAvroType(), map);
    }
    
    protected Schema createSchema(String flowName, List<FieldMetadata> fieldMetadata) {
        RecordBuilder<Schema> recordBuilder = SchemaBuilder.record("EventTable");
        
        recordBuilder = recordBuilder.prop("uuid", UUID.randomUUID().toString());
        FieldAssembler<Schema> fieldAssembler = recordBuilder.doc("Generated by " + flowName).fields();

        for (FieldMetadata fm : fieldMetadata) {
            FieldBuilder<Schema> fieldBuilder = fieldAssembler.name(fm.getFieldName());
            Pair<Type, Map<String, String>> requiredProps = getRequiredProperties(fm);
            
            Map<String, String> props = requiredProps.getSecond();
            for (Map.Entry<String, String> entry : props.entrySet()) {
                String k = entry.getKey();
                String v = entry.getValue();
                
                if (k.equals("uuid")) {
                    continue;
                }
                fieldBuilder = fieldBuilder.prop(k, v);
            }
            
            fieldBuilder.prop("uuid", UUID.randomUUID().toString());

            Type type = requiredProps.getFirst();

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

    public static class JoinCriteria {
        private final String name;
        private final FieldList joinFields;
        private final FieldList outputFields;

        public JoinCriteria(String name, FieldList joinFields, FieldList outputFields) {
            this.name = name;
            this.joinFields = joinFields;
            this.outputFields = outputFields;
        }

        public String getName() {
            return name;
        }

        public FieldList getJoinFields() {
            return joinFields;
        }

        public FieldList getOutputFields() {
            return outputFields;
        }
    }

    public static class GroupByCriteria {
        public static enum AggregationType {
            MAX, //
            MIN, //
            SUM(new FieldMetadata(Type.DOUBLE, Double.class, null, null)), //
            COUNT(new FieldMetadata(Type.LONG, Long.class, null, null)), //
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
        
        public GroupByCriteria(String aggregatedFieldName, String targetFieldName, AggregationType aggregationType) {
            this(aggregatedFieldName, targetFieldName, aggregationType, null);
        }
        
        public GroupByCriteria(AggregationType aggregationType, FieldList outputFieldStrategy) {
            this(null, null, aggregationType, outputFieldStrategy);
        }

        public GroupByCriteria(String aggregatedFieldName, String targetFieldName, AggregationType aggregationType, FieldList outputFieldStrategy) {
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

        public FieldMetadata(Schema.Type avroType, Class<?> javaType, String fieldName, Field avroField, Map<String, String> properties) {
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
        
        public static enum KindOfFields {
            ALL, //
            RESULTS, //
            GROUP;
        }
        private String[] fields;
        private KindOfFields kind;
        
        public FieldList(KindOfFields kind) {
            this.kind = kind;
        }

        public FieldList(String... fields) {
            this.fields = fields;
        }

        public String[] getFields() {
            return fields;
        }

        public List<String> getFieldsAsList() {
            if (fields == null) {
                return null;
            }
            return Arrays.<String> asList(fields);
        }

        public KindOfFields getKind() {
            return kind;
        }

    }

}
