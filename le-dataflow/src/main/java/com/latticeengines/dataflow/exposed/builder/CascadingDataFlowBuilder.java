package com.latticeengines.dataflow.exposed.builder;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import cascading.avro.AvroScheme;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.operation.Aggregator;
import cascading.operation.Function;
import cascading.operation.NoOp;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.First;
import cascading.operation.aggregator.Last;
import cascading.operation.aggregator.MaxValue;
import cascading.operation.aggregator.MinValue;
import cascading.operation.aggregator.Sum;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.filter.Not;
import cascading.pipe.Checkpoint;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.pipe.joiner.BaseJoiner;
import cascading.pipe.joiner.InnerJoin;
import cascading.pipe.joiner.LeftJoin;
import cascading.pipe.joiner.OuterJoin;
import cascading.pipe.joiner.RightJoin;
import cascading.property.AppProps;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.GlobHfs;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder.Aggregation.AggregationType;
import com.latticeengines.dataflow.runtime.cascading.AddMD5Hash;
import com.latticeengines.dataflow.runtime.cascading.AddNullColumns;
import com.latticeengines.dataflow.runtime.cascading.AddRowId;
import com.latticeengines.dataflow.runtime.cascading.GroupAndExpandFieldsBuffer;
import com.latticeengines.dataflow.runtime.cascading.JythonFunction;
import com.latticeengines.dataflow.service.impl.listener.DataFlowListener;
import com.latticeengines.dataflow.service.impl.listener.DataFlowStepListener;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;

@SuppressWarnings("rawtypes")
public abstract class CascadingDataFlowBuilder extends DataFlowBuilder {

    private static final Log log = LogFactory.getLog(CascadingDataFlowBuilder.class);
    
    private Integer counter = 1;

    private Map<String, Tap> taps = new HashMap<>();

    private Map<String, AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>>> pipesAndOutputSchemas = new HashMap<>();

    private Map<String, AbstractMap.SimpleEntry<Checkpoint, Tap>> checkpoints = new HashMap<>();
    
    private DataFlowListener dataFlowListener = new DataFlowListener();
    
    private DataFlowStepListener dataFlowStepListener = new DataFlowStepListener();

    public CascadingDataFlowBuilder() {
        this(false, false);
    }
    
    public void reset() {
        counter = 1;
        taps = new HashMap<>();
        pipesAndOutputSchemas = new HashMap<>();
        checkpoints = new HashMap<>();
    }

    public CascadingDataFlowBuilder(boolean local, boolean checkpoint) {
        super(local, checkpoint);
    }

    public Flow<?> build(FlowDef flowDef) {
        return null;
    }

    public Map<String, Tap> getSources() {
        return taps;
    }
    
    private List<FieldMetadata> getFieldMetadata(Map<String, Field> fieldMap) {
        List<FieldMetadata> fields = new ArrayList<>(fieldMap.size());

        for (Field field : fieldMap.values()) {
            Type avroType = field.schema().getTypes().get(0).getType();
            FieldMetadata fm = new FieldMetadata(avroType, AvroUtils.getJavaType(avroType), field.name(), field);
            fields.add(fm);
        }
        return fields;
    }

    private String register(Pipe pipe, List<FieldMetadata> fields) {
        return register(pipe, fields, true, null);
    }

    private String register(Pipe pipe, List<FieldMetadata> fields, boolean allowCheckpoint, String name) {
        if (name == null) {
            name = "node-" + counter++;
        }
        pipesAndOutputSchemas.put(name, new AbstractMap.SimpleEntry<>(new Pipe(name, pipe), fields));

        if (isLocal()) {
            return name;
        }
        if (isCheckpoint()) {
            DataFlowContext ctx = getDataFlowCtx();
            String ckptName = "ckpt-" + counter++;
            Checkpoint ckpt = new Checkpoint(ckptName, pipe);
            String targetPath = String.format("/tmp/checkpoints/%s/%s/%s", //
                    ctx.getProperty("CUSTOMER", String.class), //
                    ctx.getProperty("FLOWNAME", String.class), //
                    ckptName);
            Tap ckptTap = new Hfs(new SequenceFile(Fields.UNKNOWN), targetPath, SinkMode.REPLACE);
            checkpoints.put(pipe.getName(), new AbstractMap.SimpleEntry<>(ckpt, ckptTap));
            AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pipeAndFields = pipesAndOutputSchemas
                    .get(pipe.getName());
            pipesAndOutputSchemas.put(ckptName, new AbstractMap.SimpleEntry<>((Pipe) ckpt, pipeAndFields.getValue()));
            return ckptName;
        }
        return name;
    }
    
    @Override
    protected String addSource(Table sourceTable) {
        validateTableForSource(sourceTable);
        
        DataFlowContext ctx = getDataFlowCtx();
        Configuration config = ctx.getProperty("HADOOPCONF", Configuration.class);
        List<Extract> extracts = sourceTable.getExtracts();
        
        Map<String, Field> allColumns = new HashMap<>();
        Schema[] allSchemas = new Schema[extracts.size()];
        int i = 0;
        for (Extract extract : extracts) {
            String path = null;
            try {
                path = HdfsUtils.getFilesByGlob(config, extract.getPath()).get(0);
                allSchemas[i] = AvroUtils.getSchema(config, new Path(path));
                for (Field field : allSchemas[i].getFields()) {
                    allColumns.put(field.name(), field);
                }
                i++;
            } catch (IllegalArgumentException | IOException e) {
                if (path != null) {
                    throw new LedpException(LedpCode.LEDP_26006, e, new String[] { path });
                } else {
                    throw new LedpException(LedpCode.LEDP_26005, e);
                }
                
            }
        }
        
        i = 0;
        String[] sortedAllColumns = new String[allColumns.size()];
        new ArrayList<>(allColumns.keySet()).toArray(sortedAllColumns);
        Fields declaredFields = new Fields(sortedAllColumns);
        Pipe[] pipes = new Pipe[extracts.size()];
        for (Extract extract : extracts) {
            Set<String> allColumnsClone = new HashSet<>(allColumns.keySet());
            for (Field field : allSchemas[i].getFields()) {
                allColumnsClone.remove(field.name());
            }

            String source = addSource(String.format("%s-%s", sourceTable.getName(), extract.getName()), //
                    extract.getPath(), true);
            String[] extraCols = new String[allColumnsClone.size()];
            allColumnsClone.toArray(extraCols);
            
            if (allColumnsClone.size() > 0) {
                pipes[i] = new Each(new Pipe(source),new AddNullColumns(new Fields(extraCols)), //
                        declaredFields);
            } else {
                pipes[i] = new Each(new Pipe(source),new NoOp(), //
                        declaredFields);
            }
            i++;
        }
        Pipe merge = new Merge(pipes);
        String lastModifiedKeyColName = sourceTable.getLastModifiedKey().getAttributes().get(0).getName(); 
        Fields sortFields = new Fields(lastModifiedKeyColName);
        sortFields.setComparator(lastModifiedKeyColName, Collections.reverseOrder());
        
        Pipe groupby = new GroupBy(merge, new Fields(sourceTable.getPrimaryKey().getAttributeNames()), sortFields);
        Pipe first = new Every(groupby, Fields.ALL, new First(), Fields.RESULTS);
        return register(first, getFieldMetadata(allColumns), false, sourceTable.getName());
    }
    
    private void validateTableForSource(Table sourceTable) {
        if (sourceTable.getName() == null) {
            throw new LedpException(LedpCode.LEDP_26009);
        }
        
        if (sourceTable.getExtracts().size() == 0) {
            throw new LedpException(LedpCode.LEDP_26012, new String[] { sourceTable.getName() });
        }
        
        for (Extract extract : sourceTable.getExtracts()) {
            if (extract.getName() == null) {
                throw new LedpException(LedpCode.LEDP_26010, new String[] { sourceTable.getName() });
            }
            if (extract.getPath() == null) {
                throw new LedpException(LedpCode.LEDP_26011, new String[] { extract.getName(), sourceTable.getName() });
            }
        }
        
        PrimaryKey key = sourceTable.getPrimaryKey();
        
        if (key == null) {
            throw new LedpException(LedpCode.LEDP_26007, new String[] { sourceTable.getName() });
        }
        
        if (key.getAttributes().size() == 0) {
            throw new LedpException(LedpCode.LEDP_26008, new String[] { sourceTable.getName() });
        }
    }

    @Override
    protected String addSource(String sourceName, String sourcePath) {
        return addSource(sourceName, sourcePath, true);
    }

    @Override
    protected String addSource(String sourceName, String sourcePath, boolean regex) {
        Tap<?, ?, ?> tap = new GlobHfs(new AvroScheme(), sourcePath);
        if (isLocal()) {
            sourcePath = "file://" + sourcePath;
            tap = new Lfs(new AvroScheme(), sourcePath);
        }

        taps.put(sourceName, tap);

        Configuration config = getConfig();
        Schema sourceSchema = null;
        if (regex) {
            try {
                sourcePath = getSchemaPath(config, sourcePath);
                sourceSchema = AvroUtils.getSchema(config, new Path(sourcePath));
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_00002, e);
            }
        } else {
            sourceSchema = AvroUtils.getSchema(config, new Path(sourcePath));
        }

        List<FieldMetadata> fields = new ArrayList<>(sourceSchema.getFields().size());

        for (Field field : sourceSchema.getFields()) {
            Type avroType = field.schema().getTypes().get(0).getType();
            FieldMetadata fm = new FieldMetadata(avroType, AvroUtils.getJavaType(avroType), field.name(), field);
            fields.add(fm);
        }

        return register(new Pipe(sourceName), fields, false, sourceName);
    }

    private Configuration getConfig() {
        DataFlowContext ctx = getDataFlowCtx();
        Configuration config = ctx.getProperty("HADOOPCONF", Configuration.class);
        if (config == null) {
            config = new Configuration();
        }
        return config;
    }

    private String getSchemaPath(Configuration config, String sourcePath) throws Exception {
        List<String> files = HdfsUtils.getFilesByGlob(config, sourcePath);
        if (files.size() > 0) {
            sourcePath = files.get(0);
        } else {
            throw new LedpException(LedpCode.LEDP_18023);
        }
        return sourcePath;
    }

    @Override
    protected String addInnerJoin(String lhs, FieldList lhsJoinFields, String rhs, FieldList rhsJoinFields) {
        return addJoin(lhs, lhsJoinFields, rhs, rhsJoinFields, JoinType.INNER);
    }

    @Override
    protected String addLeftOuterJoin(String lhs, FieldList lhsJoinFields, String rhs, FieldList rhsJoinFields) {
        return addJoin(lhs, lhsJoinFields, rhs, rhsJoinFields, JoinType.LEFT);
    }

    @Override
    protected String addJoin(String lhs, FieldList lhsJoinFields, String rhs, FieldList rhsJoinFields, JoinType joinType) {
        List<FieldMetadata> declaredFields = new ArrayList<>();
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> lhsPipesAndFields = pipesAndOutputSchemas.get(lhs);
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> rhsPipesAndFields = pipesAndOutputSchemas.get(rhs);
        if (lhsPipesAndFields == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { lhs });
        }
        if (rhsPipesAndFields == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { rhs });
        }
        Set<String> seenFields = new HashSet<>();

        List<String> outputFields = new ArrayList<>();
        outputFields.addAll(getFieldNames(lhsPipesAndFields.getValue()));
        Map<String, FieldMetadata> nameToFieldMetadataMap = getFieldMetadataMap(lhsPipesAndFields.getValue());
        for (String fieldName : outputFields) {
            seenFields.add(fieldName);
            declaredFields.add(nameToFieldMetadataMap.get(fieldName));
        }

        outputFields = new ArrayList<>();
        outputFields.addAll(getFieldNames(rhsPipesAndFields.getValue()));
        nameToFieldMetadataMap = getFieldMetadataMap(rhsPipesAndFields.getValue());
        for (String fieldName : outputFields) {
            String originalFieldName = fieldName;

            if (seenFields.contains(fieldName)) {
                fieldName = rhs.replaceAll("\\*|-", "__") + "__" + fieldName;
            }
            seenFields.add(fieldName);
            FieldMetadata origfm = nameToFieldMetadataMap.get(originalFieldName);
            FieldMetadata fm = new FieldMetadata(origfm.getAvroType(), origfm.getJavaType(), fieldName,
                    origfm.getField(), origfm.getProperties());
            declaredFields.add(fm);
        }

        BaseJoiner joiner = null;

        switch (joinType) {
        case LEFT:
            joiner = new LeftJoin();
            break;
        case RIGHT:
            joiner = new RightJoin();
            break;
        case OUTER:
            joiner = new OuterJoin();
            break;
        default:
            joiner = new InnerJoin();
            break;
        }

        Pipe join = new CoGroup(lhsPipesAndFields.getKey(), //
                convertToFields(lhsJoinFields.getFields()), //
                rhsPipesAndFields.getKey(), //
                convertToFields(rhsJoinFields.getFields()), //
                convertToFields(getFieldNames(declaredFields)), //
                joiner);
        return register(join, declaredFields);
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
                throw new LedpException(LedpCode.LEDP_26002, new String[] { fieldName });
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
            return new First();
        case LAST:
            return new Last();
        }
        return null;
    }

    public Pipe getPipeByName(String name) {
        return pipesAndOutputSchemas.get(name).getKey();
    }

    @Override
    public Schema getSchema(String flowName, String name, DataFlowContext dataFlowCtx) {

        Schema schema = getSchemaFromFile(dataFlowCtx);
        if (schema != null) {
            return schema;
        }

        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pipeAndMetadata = pipesAndOutputSchemas.get(name);
        if (pipeAndMetadata == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { name });
        }
        return super.createSchema(flowName, pipeAndMetadata.getValue(), dataFlowCtx);
    }

    protected Schema getSchemaFromFile(DataFlowContext dataFlowCtx) {
        String taregetSchemaPath = dataFlowCtx.getProperty("TARGETSCHEMAPATH", String.class);
        if (taregetSchemaPath != null) {
            Configuration config = getConfig();
            try {
                return AvroUtils.getSchema(config, new Path(getSchemaPath(config, taregetSchemaPath)));
            } catch (Exception ex) {
                throw new LedpException(LedpCode.LEDP_26005, ex);
            }
        }
        return null;
    }

    @Override
    protected String addGroupBy(String prior, FieldList groupByFieldList, List<Aggregation> aggregation) {
        return addGroupBy(prior, groupByFieldList, null, aggregation);
    }

    @Override
    protected String addGroupBy(String prior, FieldList groupByFieldList, FieldList sortFieldList,
            List<Aggregation> aggregations) {
        List<String> groupByFields = groupByFieldList.getFieldsAsList();
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pm = pipesAndOutputSchemas.get(prior);
        if (pm == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { prior });
        }
        Map<String, FieldMetadata> nameToFieldMetadataMap = getFieldMetadataMap(pm.getValue());

        Pipe groupby = null;

        if (sortFieldList != null) {
            groupby = new GroupBy(pm.getKey(), convertToFields(groupByFields),
                    convertToFields(sortFieldList.getFieldsAsList()));
        } else {
            groupby = new GroupBy(pm.getKey(), convertToFields(groupByFields));
        }

        List<FieldMetadata> declaredFields = getIntersection(groupByFields, pipesAndOutputSchemas.get(prior).getValue());

        for (Aggregation aggregation : aggregations) {
            String aggFieldName = aggregation.getAggregatedFieldName();
            Fields outputStrategy = Fields.ALL;

            if (aggregation.getOutputFieldStrategy() != null) {
                switch (aggregation.getOutputFieldStrategy().getKind()) {
                case GROUP:
                    outputStrategy = Fields.GROUP;
                    break;
                case RESULTS:
                    outputStrategy = Fields.RESULTS;
                    break;
                default:
                    break;
                }
            }
            groupby = new Every(groupby, aggFieldName == null ? Fields.ALL : convertToFields(aggFieldName), //
                    getAggregator(aggregation.getTargetFieldName(), aggregation.getAggregationType()), //
                    outputStrategy);
            FieldMetadata fm = aggregation.getAggregationType().getFieldMetadata();

            if (aggFieldName != null) {
                if (fm == null) {
                    fm = nameToFieldMetadataMap.get(aggFieldName);
                    if (fm == null) {
                        throw new LedpException(LedpCode.LEDP_26003, new String[] { aggFieldName, prior });
                    }
                }
                FieldMetadata newfm = new FieldMetadata(fm);
                newfm.setFieldName(aggregation.getTargetFieldName());
                declaredFields.add(newfm);

            }
        }
        return register(groupby, declaredFields);
    }

    /* This method will use .avro file as schema for the sink */
    @Override
    protected String addGroupByAndExpand(String prior, FieldList groupByFieldList, String expandField,
            List<String> expandFormats, FieldList argumentsFieldList, FieldList declaredFieldList) {
        List<String> groupByFields = groupByFieldList.getFieldsAsList();
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pm = pipesAndOutputSchemas.get(prior);
        if (pm == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { prior });
        }
        Pipe groupby = null;
        groupby = new GroupBy(pm.getKey(), convertToFields(groupByFields));
        groupby = new Every(groupby, argumentsFieldList == null ? Fields.ALL
                : convertToFields(argumentsFieldList.getFieldsAsList()), //
                new GroupAndExpandFieldsBuffer(argumentsFieldList.getFieldsAsList().size(), expandField, expandFormats,
                        convertToFields(declaredFieldList.getFieldsAsList())), Fields.RESULTS);

        List<FieldMetadata> fieldMetadata = new ArrayList<FieldMetadata>();
        return register(groupby, fieldMetadata);
    }

    @Override
    protected String addFilter(String prior, String expression, FieldList filterFieldList) {
        List<String> filterFields = filterFieldList.getFieldsAsList();
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pm = pipesAndOutputSchemas.get(prior);
        String[] filterFieldsArray = new String[filterFields.size()];
        filterFields.toArray(filterFieldsArray);
        Not filter = new Not(new ExpressionFilter(expression, filterFieldsArray, getTypes(filterFields, pm.getValue())));
        Pipe each = new Each(pm.getKey(), convertToFields(filterFields), filter);
        List<FieldMetadata> fm = new ArrayList<>(pm.getValue());
        return register(each, fm);
    }

    @Override
    protected String addFunction(String prior, String expression, FieldList fieldsToApply, FieldMetadata targetField) {
        return addFunctionWithExpression(prior, expression, fieldsToApply, targetField, null);
    }

    @Override
    protected String addFunction(String prior, String expression, FieldList fieldsToApply, FieldMetadata targetField,
            FieldList outputFields) {
        return addFunctionWithExpression(prior, expression, fieldsToApply, targetField, outputFields);
    }

    private String addFunctionWithExpression(String prior, String expression, FieldList fieldsToApply,
            FieldMetadata targetField, FieldList outputFields) {
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pm = pipesAndOutputSchemas.get(prior);

        if (pm == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { prior });
        }
        ExpressionFunction function = new ExpressionFunction(new Fields(targetField.getFieldName()), //
                expression, //
                fieldsToApply.getFields(), //
                getTypes(fieldsToApply.getFieldsAsList(), pm.getValue()));

        return addFunction(prior, function, fieldsToApply, targetField, outputFields);
    }

    private String addFunction(String prior, Function<?> function, FieldList fieldsToApply, FieldMetadata targetField,
            FieldList outputFields) {
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pm = pipesAndOutputSchemas.get(prior);

        if (pm == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { prior });
        }
        Fields fieldStrategy = Fields.ALL;

        List<FieldMetadata> fm = new ArrayList<>(pm.getValue());

        if (fieldsToApply.getFields().length == 1 && fieldsToApply.getFields()[0].equals(targetField.getFieldName())) {
            fieldStrategy = Fields.REPLACE;
        }

        if (outputFields != null) {
            fieldStrategy = convertToFields(outputFields.getFields());
        }
        Pipe each = new Each(pm.getKey(), convertToFields(fieldsToApply.getFieldsAsList()), function, fieldStrategy);

        if (fieldStrategy != Fields.REPLACE) {
            fm.add(targetField);
            fm = retainOutputFields(outputFields, fm);
        } else {
            Map<String, FieldMetadata> nameToFieldMetadataMap = getFieldMetadataMap(fm);
            FieldMetadata targetFm = nameToFieldMetadataMap.get(targetField.getFieldName());

            if (targetFm.getJavaType() != targetField.getJavaType()) {
                FieldMetadata replaceFm = new FieldMetadata(targetField.getAvroType(), targetField.getJavaType(),
                        targetField.getFieldName(), null);
                nameToFieldMetadataMap.put(targetField.getFieldName(), replaceFm);
                for (int i = 0; i < fm.size(); i++) {
                    if (fm.get(i).getFieldName().equals(replaceFm.getFieldName())) {
                        fm.set(i, replaceFm);
                    }
                }
            }
        }

        return register(each, fm);
    }

    protected String addRetainFunction(String prior, FieldList outputFields) {
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pm = pipesAndOutputSchemas.get(prior);
        if (pm == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { prior });
        }

        Pipe retain = new Retain(pm.getKey(), convertToFields(outputFields.getFields()));

        List<FieldMetadata> fm = new ArrayList<>(pm.getValue());
        fm = retainOutputFields(outputFields, fm);
        return register(retain, fm);
    }

    private List<FieldMetadata> retainOutputFields(FieldList outputFields, List<FieldMetadata> fm) {
        if (outputFields != null) {
            List<FieldMetadata> newFieldMetadata = new ArrayList<>();
            Map<String, FieldMetadata> nameToFieldMetadataMap = getFieldMetadataMap(fm);
            Set<String> metadataKeySet = nameToFieldMetadataMap.keySet();
            List<String> outputFieldList = outputFields.getFieldsAsList();
            for (String outputField : outputFieldList) {
                if (metadataKeySet.contains(outputField)) {
                    newFieldMetadata.add(nameToFieldMetadataMap.get(outputField));
                }
            }
            return newFieldMetadata;
        }
        return fm;
    }

    @Override
    protected String addMD5(String prior, FieldList fieldsToApply, String targetFieldName) {
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pm = pipesAndOutputSchemas.get(prior);
        if (pm == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { prior });
        }
        Pipe each = new Each(pm.getKey(), convertToFields(fieldsToApply.getFields()), new AddMD5Hash(new Fields(
                targetFieldName)), Fields.ALL);
        List<FieldMetadata> newFm = new ArrayList<>(pm.getValue());
        FieldMetadata pdHashFm = new FieldMetadata(Type.STRING, String.class, targetFieldName, null);
        pdHashFm.setPropertyValue("length", "32");
        pdHashFm.setPropertyValue("precision", "0");
        pdHashFm.setPropertyValue("scale", "0");
        pdHashFm.setPropertyValue("displayName", "Prop Data Hash");
        newFm.add(pdHashFm);

        return register(each, newFm);
    }

    @Override
    protected String addRowId(String prior, String targetFieldName, String tableName) {
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pm = pipesAndOutputSchemas.get(prior);
        if (pm == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { prior });
        }
        Pipe each = new Each(pm.getKey(), Fields.ALL, new AddRowId(new Fields(targetFieldName), tableName), Fields.ALL);
        List<FieldMetadata> newFm = new ArrayList<>(pm.getValue());
        FieldMetadata rowIdFm = new FieldMetadata(Type.LONG, Long.class, targetFieldName, null);
        rowIdFm.setPropertyValue("logicalType", "rowid");
        rowIdFm.setPropertyValue("length", "0");
        rowIdFm.setPropertyValue("precision", "0");
        rowIdFm.setPropertyValue("scale", "0");
        rowIdFm.setPropertyValue("displayName", "Row ID");
        newFm.add(rowIdFm);

        return register(each, newFm);
    }

    @Override
    protected String addJythonFunction(String prior, String scriptName, String functionName, FieldList fieldsToApply,
            FieldMetadata targetField) {
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pm = pipesAndOutputSchemas.get(prior);
        if (pm == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { prior });
        }
        return addFunction(prior, //
                new JythonFunction(scriptName, //
                        functionName, //
                        targetField.getJavaType(), //
                        convertToFields(fieldsToApply.getFields()), //
                        convertToFields(targetField.getFieldName())), //
                fieldsToApply, //
                targetField, null);
    }

    @Override
    protected List<FieldMetadata> getMetadata(String operator) {
        return pipesAndOutputSchemas.get(operator).getValue();
    }

    @Override
    public void runFlow(DataFlowContext dataFlowCtx) {
        @SuppressWarnings("unchecked")
        Map<String, String> sourcePaths = dataFlowCtx.getProperty("SOURCES", Map.class);
        @SuppressWarnings("unchecked")
        Map<String, Table> sourceTables = dataFlowCtx.getProperty("SOURCETABLES", Map.class);
        String flowName = dataFlowCtx.getProperty("FLOWNAME", String.class);
        String targetPath = dataFlowCtx.getProperty("TARGETPATH", String.class);
        Properties jobProperties = dataFlowCtx.getProperty("JOBPROPERTIES", Properties.class);
        String engineType = dataFlowCtx.getProperty("ENGINE", String.class);
        ExecutionEngine engine = ExecutionEngine.get(engineType);

        String lastOperator = null;
        if (sourceTables != null) {
            lastOperator = constructFlowDefinition(dataFlowCtx, sourcePaths, sourceTables);
        } else {
            lastOperator = constructFlowDefinition(dataFlowCtx, sourcePaths);
        }
        
        Schema schema = getSchema(flowName, lastOperator, dataFlowCtx);
        System.out.println(schema);
        Tap<?, ?, ?> sink = new Lfs(new AvroScheme(schema), targetPath, SinkMode.KEEP);
        if (!isLocal()) {
            sink = new Hfs(new AvroScheme(schema), targetPath, SinkMode.KEEP);
        }
        
        
        Properties properties = new Properties();
        if (jobProperties != null) {
            properties.putAll(jobProperties);
        }
        AppProps.setApplicationJarClass(properties, getClass());
        FlowConnector flowConnector = engine.createFlowConnector(dataFlowCtx, properties);

        FlowDef flowDef = FlowDef.flowDef().setName(flowName) //
                .addSources(getSources()) //
                .addTailSink(getPipeByName(lastOperator), sink);
        
        for (AbstractMap.SimpleEntry<Checkpoint, Tap> entry : checkpoints.values()) {
            flowDef = flowDef.addCheckpoint(entry.getKey(), entry.getValue());
        }
        DataFlowContext ctx = getDataFlowCtx();
        Configuration config = ctx.getProperty("HADOOPCONF", Configuration.class);

        try {
            List<String> files = HdfsUtils.getFilesForDir(config, "/app/dataflow/lib/");
            for (String file : files) {
                flowDef.addToClassPath(file);
            }
        } catch (Exception e) {
            log.warn("Exception retrieving library jars for this flow.", e);
        }

        Flow<?> flow = flowConnector.connect(flowDef);

        flow.writeDOT("dot/wcr.dot");
        flow.addListener(dataFlowListener);
        flow.addStepListener(dataFlowStepListener);
        flow.complete();
    }

    public static class RegexFilter implements HdfsUtils.HdfsFileFilter {

        private Pattern pattern;

        public RegexFilter(String glob) {
            String regex = createRegexFromGlob(glob);
            pattern = Pattern.compile(regex);
        }

        @Override
        public boolean accept(FileStatus file) {
            String filePath = Path.getPathWithoutSchemeAndAuthority(file.getPath()).toString();
            return pattern.matcher(filePath).matches();
        }
    }

    private static String createRegexFromGlob(String glob) {
        String out = "^";
        for (int i = 0; i < glob.length(); ++i) {
            final char c = glob.charAt(i);
            switch (c) {
            case '*':
                out += ".*";
                break;
            case '?':
                out += '.';
                break;
            case '.':
                out += "\\.";
                break;
            case '\\':
                out += "\\\\";
                break;
            default:
                out += c;
            }
        }
        out += '$';
        return out;
    }

}
