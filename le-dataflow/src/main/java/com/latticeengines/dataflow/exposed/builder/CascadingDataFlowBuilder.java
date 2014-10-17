package com.latticeengines.dataflow.exposed.builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import cascading.avro.AvroScheme;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.operation.Aggregator;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.First;
import cascading.operation.aggregator.Last;
import cascading.operation.aggregator.MaxValue;
import cascading.operation.aggregator.MinValue;
import cascading.operation.aggregator.Sum;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.filter.Not;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.InnerJoin;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.util.Pair;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder.GroupByCriteria.AggregationType;
import com.latticeengines.dataflow.exposed.exception.DataFlowException;

public abstract class CascadingDataFlowBuilder extends DataFlowBuilder {

    private boolean local = false;
    
    @SuppressWarnings("rawtypes")
    private Map<String, Tap> taps = new HashMap<>();
    
    private Map<String, Schema> schemas = new HashMap<>();
    
    private Map<String, Pair<Pipe, List<FieldMetadata>>> pipesAndOutputSchemas = new HashMap<>();
    
    public CascadingDataFlowBuilder() {
        this(false);
    }
    
    public CascadingDataFlowBuilder(boolean local) {
        this.local = local;
    }
    
    public Flow<?> build(FlowDef flowDef) {
        return null;
    }
    
    @SuppressWarnings("rawtypes")
    public Map<String, Tap> getSources() {
        return taps;
    }
    
    @Override
    protected void addSource(String sourceName, String sourcePath) {
        Tap<?, ?, ?> tap = new Hfs(new AvroScheme(), sourcePath);
        if (local) {
            sourcePath = "file://" + sourcePath;
            tap = new Lfs(new AvroScheme(), sourcePath);
        }
        
        taps.put(sourceName, tap);
        
        Schema sourceSchema = AvroUtils.getSchema(new Configuration(), new Path(sourcePath));
        List<FieldMetadata> fields = new ArrayList<>(sourceSchema.getFields().size());

        for (Field field : sourceSchema.getFields()) {
            Type avroType = field.schema().getTypes().get(0).getType();
            FieldMetadata fm = new FieldMetadata(avroType, AvroUtils.getJavaType(avroType), field.name());
            fields.add(fm);
        }
        pipesAndOutputSchemas.put(sourceName, new Pair<>(new Pipe(sourceName), fields));
        schemas.put(sourceName, sourceSchema);
    }
    
    @Override
    protected String addInnerJoin(List<JoinCriteria> joinCriteria) {
        List<FieldMetadata> declaredFields = new ArrayList<>();
        Set<String> seenFields = new HashSet<>();
        Pipe[] pipes = new Pipe[joinCriteria.size()];
        Fields[] joinFields = new Fields[joinCriteria.size()];
        int i = 0;
        Map<String, FieldMetadata> nameToFieldMetadataMap = null; 
        for (JoinCriteria joinCriterion : joinCriteria) {
            String name = joinCriterion.getName();
            FieldList outputFieldList = joinCriterion.getOutputFields();
            
            List<String> outputFields = null;
            
            if (outputFieldList == null) {
                
                Pair<Pipe, List<FieldMetadata>> pipesAndFields = pipesAndOutputSchemas.get(name);
                
                if (pipesAndFields == null) {
                    throw new DataFlowException("Unseen prior pipe " + name);
                } else {
                    nameToFieldMetadataMap = getFieldMetadataMap(pipesAndFields.getRhs());
                }
                outputFields = new ArrayList<>();
                outputFields.addAll(getFieldNames(pipesAndFields.getRhs()));
            } else {
                outputFields = outputFieldList.getFieldsAsList();
            }
            for (String fieldName : outputFields) {
                String originalFieldName = fieldName;
                if (seenFields.contains(fieldName)) {
                    fieldName = fieldName + "_1";
                }
                
                seenFields.add(fieldName);
                FieldMetadata fm = nameToFieldMetadataMap.get(originalFieldName);
                declaredFields.add(new FieldMetadata(fm.getAvroType(), fm.getJavaType(), fieldName));
            }
            pipes[i] = getPipeByName(name);
            joinFields[i] = convertToFields(joinCriterion.getJoinFields().getFields());
            i++;
        }
        Pipe join = new CoGroup(pipes, joinFields, convertToFields(getFieldNames(declaredFields)), new InnerJoin());
        pipesAndOutputSchemas.put(join.getName(), new Pair<>(join, declaredFields));
        return join.getName();
    }
    
    private static Class<?>[] getTypes(List<String> fieldNames, List<FieldMetadata> full) {
        List<FieldMetadata> fmList = getIntersection(fieldNames, full);
        
        Class<?>[] types = new Class[fmList.size()];
        
        int i = 0;
        for (FieldMetadata fm : fmList) {
            types[i++] = fm.getJavaType();
        }
        return types;
    }
    
    private static List<FieldMetadata> getIntersection(List<String> partial, List<FieldMetadata> full) {
        Map<String, FieldMetadata> nameToFieldMetadataMap = getFieldMetadataMap(full);
        List<FieldMetadata> partialFieldMetadata = new ArrayList<>();
        
        for (String fieldName : partial) {
            FieldMetadata fm = nameToFieldMetadataMap.get(fieldName);
            
            if (fm == null) {
                throw new DataFlowException("Unknown field name " + fieldName + " from previous pipe.");
            }
            partialFieldMetadata.add(fm);
        }
        return partialFieldMetadata;
    }

    private static Map<String, FieldMetadata> getFieldMetadataMap(List<FieldMetadata> fieldMetadata) {
        Map<String, FieldMetadata> nameToFieldMetadataMap = new HashMap<>();
        
        for (FieldMetadata fieldMetadatum : fieldMetadata) {
            nameToFieldMetadataMap.put(fieldMetadatum.getFieldName(), fieldMetadatum);
        }
        return nameToFieldMetadataMap;
    }
    
    private static List<String> getFieldNames(List<FieldMetadata> fieldMetadata) {
        List<String> fieldNames = new ArrayList<>();
        for (FieldMetadata fieldMetadatum : fieldMetadata) {
            fieldNames.add(fieldMetadatum.getFieldName());
        }
        return fieldNames;
    }
    
    private static Fields convertToFields(List<String> fields) {
        String[] fieldsArray = new String[fields.size()];
        fields.toArray(fieldsArray);
        return new Fields(fieldsArray);
    }
    

    private static Fields convertToFields(String[] fields) {
        return new Fields(fields);
    }
    
    private static Fields convertToFields(String field) {
        return new Fields(field);
    }
    
    private static Aggregator<?> getAggregator(String aggregatedFieldName, AggregationType aggregationType) {
        switch (aggregationType) {
        case MAX:
            return new MaxValue(convertToFields(aggregatedFieldName));
        case MIN:
            return new MinValue(convertToFields(aggregatedFieldName));
        case COUNT:
            return new Count(convertToFields(aggregatedFieldName));
        case SUM:
            return new Sum(convertToFields(aggregatedFieldName));
        case FIRST:
            return new First(convertToFields(aggregatedFieldName));
        case LAST:
            return new Last(convertToFields(aggregatedFieldName));
        }
        return null;
    }

    public Pipe getPipeByName(String name) {
        return pipesAndOutputSchemas.get(name).getLhs();
    }
    
    @Override
    public Schema getSchema(String name) {
        Pair<Pipe, List<FieldMetadata>> pipeAndMetadata = pipesAndOutputSchemas.get(name);
        
        if (pipeAndMetadata == null) {
            throw new DataFlowException("Unseen prior pipe " + name);
        }

        List<FieldMetadata> fm = pipeAndMetadata.getRhs();
        
        return null;
    }
    
    @Override
    protected String addGroupBy(String prior, FieldList groupByFieldList, List<GroupByCriteria> groupByCriteria) {
        List<String> groupByFields = groupByFieldList.getFieldsAsList();
        Pair<Pipe, List<FieldMetadata>> pm = pipesAndOutputSchemas.get(prior);
        if (pm == null) {
            throw new DataFlowException("Unseen prior pipe " + prior + ".");
        }
        Map<String, FieldMetadata> nameToFieldMetadataMap = getFieldMetadataMap(pm.getRhs());
        Pipe groupby = new GroupBy(pm.getLhs(), convertToFields(groupByFields));
        List<FieldMetadata> declaredFields = getIntersection(groupByFields, pipesAndOutputSchemas.get(prior).getRhs());

        for (GroupByCriteria groupByCriterion : groupByCriteria) {
            String aggFieldName = groupByCriterion.getAggregatedFieldName();
            groupby = new Every(groupby, convertToFields(aggFieldName), //
                    getAggregator(groupByCriterion.getTargetFieldName(), groupByCriterion.getAggregationType()), //
                    Fields.ALL);
            Type avroType = groupByCriterion.getAggregationType().getAvroType();
            Class<?> javaType = groupByCriterion.getAggregationType().getJavaType();
            
            FieldMetadata fmForAggFieldName = nameToFieldMetadataMap.get(aggFieldName);
            if (fmForAggFieldName == null) {
                throw new DataFlowException("Field name " + aggFieldName + " not found in prior pipe.");
            }
            
            if (avroType == null) {
                avroType = fmForAggFieldName.getAvroType();
                javaType = fmForAggFieldName.getJavaType();
            }
            declaredFields.add(new FieldMetadata(avroType, javaType, groupByCriterion.getTargetFieldName()));
        }
        
        pipesAndOutputSchemas.put(groupby.getName(), new Pair<>(groupby, declaredFields));
        

        return groupby.getName();
    }
    
    protected String addTarget(String targetName, Map<String, List<String>> targetFieldNames) {
        return null;
    }
    
    @Override
    protected String addFilter(String prior, String expression, FieldList filterFieldList) {
        List<String> filterFields = filterFieldList.getFieldsAsList();
        Pair<Pipe, List<FieldMetadata>> pm = pipesAndOutputSchemas.get(prior);
        String[] filterFieldsArray = new String[filterFields.size()];
        filterFields.toArray(filterFieldsArray);
        Not filter = new Not(new ExpressionFilter(expression, filterFieldsArray, getTypes(filterFields, pm.getRhs())));
        Pipe each = new Each(pm.getLhs(), convertToFields(filterFields), filter);
        pipesAndOutputSchemas.put(each.getName(), new Pair<>(each, (List<FieldMetadata>) null));
        return each.getName();
    }
    
    @Override
    protected String addFunction(String prior) {
        return null;
    }
    
    
}
