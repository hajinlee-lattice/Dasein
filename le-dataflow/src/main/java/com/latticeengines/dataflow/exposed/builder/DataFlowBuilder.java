package com.latticeengines.dataflow.exposed.builder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

public abstract class DataFlowBuilder {
    
    private boolean local;

    public abstract String constructFlowDefinition(Map<String, String> sources);
    
    public abstract Schema getSchema(String name);
    
    public abstract void runFlow(DataFlowContext dataFlowCtx);
    
    protected abstract void addSource(String sourceName, String sourcePath);
    
    protected abstract String addInnerJoin(List<JoinCriteria> joinCriteria);
    
    protected abstract String addGroupBy(String prior, FieldList groupByFields, List<GroupByCriteria> groupByCriteria);
    
    protected abstract String addFilter(String prior, String expression, FieldList filterFields);
    
    protected abstract String addFunction(String prior, String expression, FieldList fieldsToApply, String fieldToCreate, Class<?> fieldDataTypeToCreate);

    
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
            SUM, //
            COUNT(Type.INT, Integer.class), //
            FIRST, //
            LAST;
            
            private Type avroType;
            private Class<?> javaType;
            
            AggregationType() {
                this(null, null);
            }
            
            AggregationType(Type avroType, Class<?> javaType) {
                this.avroType = avroType;
                this.javaType = javaType;
            }

            public Type getAvroType() {
                return avroType;
            }
            
            public Class<?> getJavaType() {
                return javaType;
            }
        }
        private final String aggregatedFieldName;
        private final String targetFieldName;
        private final AggregationType aggregationType;
        
        public GroupByCriteria(String aggregatedFieldName, String targetFieldName, AggregationType aggregationType) {
            this.aggregatedFieldName = aggregatedFieldName;
            this.targetFieldName = targetFieldName;
            this.aggregationType = aggregationType;
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
    }
    
    public static class FieldMetadata {
        private final Schema.Type avroType;
        private final Class<?> javaType;
        private final String fieldName;
        
        public FieldMetadata(Schema.Type avroType, Class<?> javaType, String fieldName) {
            this.avroType = avroType;
            this.javaType = javaType;
            this.fieldName = fieldName;
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
    }
    
    public static class FieldList {
        private final String[] fields;
        
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
            return Arrays.<String>asList(fields);
        }
    }


}
