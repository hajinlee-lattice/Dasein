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
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;

import cascading.avro.AvroScheme;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.operation.Aggregator;
import cascading.operation.Buffer;
import cascading.operation.Function;
import cascading.operation.NoOp;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.First;
import cascading.operation.aggregator.Last;
import cascading.operation.aggregator.MaxValue;
import cascading.operation.aggregator.MinValue;
import cascading.operation.aggregator.Sum;
import cascading.operation.buffer.FirstNBuffer;
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
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Rename;
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
import cascading.tuple.Fields;

import com.google.common.base.Joiner;
import com.latticeengines.common.exposed.query.Sort;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder.Aggregation.AggregationType;
import com.latticeengines.dataflow.exposed.builder.operations.LimitOperation;
import com.latticeengines.dataflow.exposed.builder.operations.MergeOperation;
import com.latticeengines.dataflow.exposed.builder.operations.Operation;
import com.latticeengines.dataflow.exposed.builder.operations.SortOperation;
import com.latticeengines.dataflow.runtime.cascading.AddMD5Hash;
import com.latticeengines.dataflow.runtime.cascading.AddNullColumns;
import com.latticeengines.dataflow.runtime.cascading.AddRowId;
import com.latticeengines.dataflow.runtime.cascading.GroupAndExpandFieldsBuffer;
import com.latticeengines.dataflow.runtime.cascading.JythonFunction;
import com.latticeengines.dataflow.service.impl.listener.DataFlowListener;
import com.latticeengines.dataflow.service.impl.listener.DataFlowStepListener;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;

@SuppressWarnings("rawtypes")
public abstract class CascadingDataFlowBuilder extends DataFlowBuilder {

    private static final Log log = LogFactory.getLog(CascadingDataFlowBuilder.class);

    protected static class Node {
        private String identifier;
        private CascadingDataFlowBuilder builder;

        private Node(String identifier, CascadingDataFlowBuilder builder) {
            this.identifier = identifier;
            this.builder = builder;
        }

        public Node join(FieldList lhsJoinFields, Node rhs, FieldList rhsJoinFields, JoinType joinType) {
            return new Node(builder.addJoin(identifier, lhsJoinFields, rhs.identifier, rhsJoinFields, joinType),
                    builder);
        }

        public Node innerJoin(FieldList lhsJoinFields, Node rhs, FieldList rhsJoinFields) {
            return new Node(builder.addInnerJoin(identifier, lhsJoinFields, rhs.identifier, rhsJoinFields), builder);
        }

        public Node innerJoin(String lhsField, Node rhs, String rhsField) {
            return innerJoin(new FieldList(lhsField), rhs, new FieldList(rhsField));
        }

        public Node leftOuterJoin(FieldList lhsJoinFields, Node rhs, FieldList rhsJoinFields) {
            return new Node(builder.addLeftOuterJoin(identifier, lhsJoinFields, rhs.identifier, rhsJoinFields), builder);
        }

        public Node leftOuterJoin(String lhsField, Node rhs, String rhsField) {
            return leftOuterJoin(new FieldList(lhsField), rhs, new FieldList(rhsField));
        }

        public Node groupBy(FieldList groupByFieldList, List<Aggregation> aggregations) {
            return new Node(builder.addGroupBy(identifier, groupByFieldList, aggregations), builder);
        }

        public Node groupBy(FieldList groupByFieldList, FieldList sortFieldList, List<Aggregation> aggregations) {
            return new Node(builder.addGroupBy(identifier, groupByFieldList, sortFieldList, aggregations), builder);
        }

        public Node groupByAndLimit(FieldList groupByFieldList, int count) {
            return new Node(builder.addGroupByAndBuffer(identifier, groupByFieldList, new FirstNBuffer(count)), builder);
        }

        public Node groupByAndLimit(FieldList groupByFieldList, FieldList sortFieldList, int count, boolean descending) {
            return new Node(builder.addGroupByAndBuffer(identifier, groupByFieldList, sortFieldList, new FirstNBuffer(
                    count), descending), builder);
        }

        public Node groupByAndBuffer(FieldList groupByFieldList, FieldList sortFieldList, Buffer buffer,
                boolean descending) {
            return new Node(
                    builder.addGroupByAndBuffer(identifier, groupByFieldList, sortFieldList, buffer, descending),
                    builder);
        }

        public Node groupByAndBuffer(FieldList groupByFieldList, Buffer buffer) {
            return new Node(builder.addGroupByAndBuffer(identifier, groupByFieldList, buffer), builder);
        }

        public Node groupByAndExpand(FieldList groupByFieldList, String expandField, List<String> expandFormats, //
                FieldList argumentsFieldList, FieldList declaredFieldList) {
            return new Node(builder.addGroupByAndExpand(identifier, groupByFieldList, expandField, //
                    expandFormats, argumentsFieldList, declaredFieldList), builder);
        }

        public static Node groupByAndFirst(Node[] priors, Fields groupByFields, Fields sortFields) {
            if (priors.length == 0) {
                throw new IllegalArgumentException("priors must have a non-zero length");
            }
            CascadingDataFlowBuilder builder = priors[0].builder;

            List<String> priorIdentifiers = new ArrayList<>();
            for (Node n : priors) {
                priorIdentifiers.add(n.identifier);
            }
            return new Node(
                    builder.addGroupByAndFirst((String[]) priorIdentifiers.toArray(), groupByFields, sortFields),
                    builder);
        }

        public Node sort(String field) {
            return new Node(builder.register(new SortOperation(identifier, field, builder)), builder);
        }

        public Node sort(String field, boolean descending) {
            return new Node(builder.register(new SortOperation(identifier, field, descending, builder)), builder);
        }

        public Node sort(Sort sort) {
            return new Node(builder.register(new SortOperation(identifier, sort, builder)), builder);
        }

        public Node filter(String expression, FieldList filterFieldList) {
            return new Node(builder.addFilter(identifier, expression, filterFieldList), builder);
        }

        public Node addFunction(String expression, FieldList fieldsToApply, FieldMetadata targetField) {
            return new Node(builder.addFunction(identifier, expression, fieldsToApply, targetField), builder);
        }

        public Node addFunction(String expression, FieldList fieldsToApply, FieldMetadata targetField,
                FieldList outputFields) {
            return new Node(builder.addFunction(identifier, expression, fieldsToApply, targetField, outputFields),
                    builder);
        }

        public Node addMD5(FieldList fieldsToApply, String targetFieldName) {
            return new Node(builder.addMD5(identifier, fieldsToApply, targetFieldName), builder);
        }

        public Node addRowID(String targetFieldName) {
            return new Node(builder.addRowId(identifier, targetFieldName), builder);
        }

        public Node addJythonFunction(String scriptName, String functionName, FieldList fieldsToApply,
                FieldMetadata targetField) {
            return new Node(
                    builder.addJythonFunction(identifier, scriptName, functionName, fieldsToApply, targetField),
                    builder);
        }

        public Node renamePipe(String newname) {
            return new Node(builder.addRenamePipe(identifier, newname), builder);
        }

        public Node retain(FieldList outputFields) {
            return new Node(builder.addRetain(identifier, outputFields), builder);
        }

        public Node discard(FieldList toDiscard) {
            return new Node(builder.addDiscard(identifier, toDiscard), builder);
        }

        public Node checkpoint(String name) {
            return new Node(builder.addCheckpoint(identifier, name), builder);
        }

        public Node rename(FieldList previousNames, FieldList newNames) {
            return new Node(builder.addRename(identifier, previousNames, newNames), builder);
        }

        public Node stopList(Node rhs, String lhsFieldName, String rhsFieldName) {
            return new Node(builder.addStopListFilter(identifier, rhs.identifier, lhsFieldName, rhsFieldName), builder);
        }

        public Node merge(Node rhs) {
            return new Node(builder.register(new MergeOperation(identifier, rhs.identifier, builder)), builder);
        }

        public Node limit(int count) {
            return new Node(builder.register(new LimitOperation(identifier, count, builder)), builder);
        }

        public List<FieldMetadata> getSchema() {
            return builder.getMetadata(identifier);
        }

        public String getPipeName() {
            return builder.getPipeByIdentifier(identifier).getName();
        }

        public List<String> getFieldNames() {
            List<String> names = new ArrayList<>();
            List<FieldMetadata> fields = getSchema();
            for (FieldMetadata field : fields) {
                names.add(field.getFieldName());
            }
            return names;
        }

        private String getIdentifier() {
            return identifier;
        }
    }

    private Integer counter = 1;

    private Map<String, Tap> taps = new HashMap<>();

    private Map<String, AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>>> pipesAndOutputSchemas = new HashMap<>();

    private Map<String, AbstractMap.SimpleEntry<Checkpoint, Tap>> checkpoints = new HashMap<>();

    private DataFlowListener dataFlowListener = new DataFlowListener();

    private DataFlowStepListener dataFlowStepListener = new DataFlowStepListener();

    public void reset() {
        counter = 1;
        taps = new HashMap<>();
        pipesAndOutputSchemas = new HashMap<>();
        checkpoints = new HashMap<>();
    }

    public Map<String, Tap> getSources() {
        return taps;
    }

    // Non-typesafe
    public abstract String constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources);

    // Typesafe
    public abstract Node constructFlowDefinition(DataFlowParameters parameters);

    public String register(Operation operation) {
        return register(operation.getOutputPipe(), operation.getOutputMetadata());
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
        return register(pipe, fields, null);
    }

    private String register(Pipe pipe, List<FieldMetadata> fields, String lookupId) {
        if (lookupId == null) {
            lookupId = "node-" + counter++;
        }
        pipesAndOutputSchemas.put(lookupId, new AbstractMap.SimpleEntry<>(pipe, fields));
        return lookupId;
    }

    protected String addCheckpoint(String prior, String name) {
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pipeAndMetadata = pipesAndOutputSchemas.get(prior);
        if (pipeAndMetadata == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { prior });
        }
        Pipe pipe = pipeAndMetadata.getKey();

        if (isCheckpoint()) {
            Checkpoint ckpt = new Checkpoint(name, pipe);
            Tap ckptSink = createCheckpointSink(name);
            checkpoints.put(pipe.getName(), new AbstractMap.SimpleEntry<>(ckpt, ckptSink));
            return register(ckpt, pipeAndMetadata.getValue());
        }

        return prior;
    }

    protected Node addSource(String sourceTableName) {
        DataFlowContext ctx = getDataFlowCtx();
        @SuppressWarnings("unchecked")
        Map<String, Table> sourceTables = ctx.getProperty("SOURCETABLES", Map.class);
        Table sourceTable = sourceTables.get(sourceTableName);
        if (sourceTable == null) {
            throw new RuntimeException(String.format("Could not find source with name %s", sourceTableName));
        }

        validateTableForSource(sourceTable);
        Configuration config = ctx.getProperty("HADOOPCONF", Configuration.class);

        List<Extract> extracts = sourceTable.getExtracts();

        Map<String, Field> allColumns = new HashMap<>();
        Schema[] allSchemas = new Schema[extracts.size()];
        int i = 0;
        for (Extract extract : extracts) {
            String path = null;
            try {
                List<String> matches = HdfsUtils.getFilesByGlob(config, extract.getPath());
                if (matches.size() == 0) {
                    throw new IllegalStateException(String.format("Could not find extract with path %s in HDFS",
                            extract.getPath()));
                }
                path = matches.get(0);
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

            String source = addSource(String.format("%s-%s", sourceTableName, extract.getName()), //
                    extract.getPath(), true);
            String[] extraCols = new String[allColumnsClone.size()];
            allColumnsClone.toArray(extraCols);

            if (allColumnsClone.size() > 0) {
                pipes[i] = new Each(new Pipe(source), new AddNullColumns(new Fields(extraCols)), //
                        declaredFields);
            } else {
                pipes[i] = new Each(new Pipe(source), new NoOp(), //
                        declaredFields);
            }
            i++;
        }

        Pipe toRegister = new Merge(pipes);

        // group and sort the extracts
        if (sourceTable.getPrimaryKey() != null && sourceTable.getLastModifiedKey() != null) {
            // TODO Use logical sort
            String lastModifiedKeyColName = sourceTable.getLastModifiedKey().getAttributes().get(0);
            Fields sortFields = new Fields(lastModifiedKeyColName);
            sortFields.setComparator(lastModifiedKeyColName, Collections.reverseOrder());

            Pipe groupby = new GroupBy(toRegister, new Fields(sourceTable.getPrimaryKey().getAttributeNames()),
                    sortFields);
            toRegister = new Every(groupby, Fields.ALL, new First(), Fields.RESULTS);
        }
        toRegister = new Pipe(sourceTableName, toRegister);

        return new Node(register(toRegister, getFieldMetadata(allColumns), sourceTableName), this);
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

        if (sourceTable.getExtracts().size() > 1) {
            PrimaryKey key = sourceTable.getPrimaryKey();

            if (key == null) {
                throw new LedpException(LedpCode.LEDP_26007, new String[] { sourceTable.getName() });
            }

            LastModifiedKey lmk = sourceTable.getLastModifiedKey();

            if (lmk == null) {
                throw new LedpException(LedpCode.LEDP_26013, new String[] { sourceTable.getName() });
            }
        }

        if (sourceTable.getPrimaryKey() != null) {
            if (sourceTable.getPrimaryKey().getAttributes().size() == 0) {
                throw new LedpException(LedpCode.LEDP_26008, new String[] { sourceTable.getName() });
            }
        }
    }

    protected String addSource(String sourceName, String sourcePath) {
        return addSource(sourceName, sourcePath, true);
    }

    protected String addSource(String sourceName, String sourcePath, boolean regex) {
        Tap<?, ?, ?> tap = createTap(sourcePath);

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

        return register(new Pipe(sourceName), fields, sourceName);
    }

    protected String joinFieldName(String identifier, String fieldName) {
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> lookup = pipesAndOutputSchemas.get(identifier);
        if (lookup == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { identifier });
        }
        return lookup.getKey().getName().replaceAll("\\*|-", "__") + "__" + fieldName;
    }

    protected String addRenamePipe(String prior, String newname) {
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> priorLookup = pipesAndOutputSchemas.get(prior);
        if (priorLookup == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { prior });
        }
        Pipe pipe = new Pipe(newname, priorLookup.getKey());
        return register(pipe, priorLookup.getValue());
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

    protected String addInnerJoin(String lhs, FieldList lhsJoinFields, String rhs, FieldList rhsJoinFields) {
        return addJoin(lhs, lhsJoinFields, rhs, rhsJoinFields, JoinType.INNER);
    }

    protected String addLeftOuterJoin(String lhs, FieldList lhsJoinFields, String rhs, FieldList rhsJoinFields) {
        return addJoin(lhs, lhsJoinFields, rhs, rhsJoinFields, JoinType.LEFT);
    }

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
                fieldName = joinFieldName(rhs, fieldName);
                if (seenFields.contains(fieldName)) {
                    throw new RuntimeException(
                            String.format(
                                    "Cannot create joinFieldName %s from field name %s because a field with that name already exists.  Discard the field to avoid this error",
                                    fieldName, originalFieldName));
                }
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

    public Pipe getPipeByIdentifier(String identifier) {
        return pipesAndOutputSchemas.get(identifier).getKey();
    }

    public AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> getPipeAndMetadata(String identifier) {
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pipeAndMetadata = pipesAndOutputSchemas.get(identifier);
        if (pipeAndMetadata == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { identifier });
        }
        return pipeAndMetadata;
    }

    public Schema getSchema(String flowName, String identifier, DataFlowContext dataFlowCtx) {

        Schema schema = getSchemaFromFile(dataFlowCtx);
        if (schema != null) {
            return schema;
        }

        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pipeAndMetadata = pipesAndOutputSchemas.get(identifier);
        if (pipeAndMetadata == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { identifier });
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

    protected String addGroupBy(String prior, FieldList groupByFieldList, List<Aggregation> aggregation) {
        return addGroupBy(prior, groupByFieldList, null, aggregation);
    }

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

    protected String addGroupByAndBuffer(String prior, FieldList groupByFields, Buffer buffer) {
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pm = pipesAndOutputSchemas.get(prior);
        if (pm == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { prior });
        }
        Pipe groupby = null;
        groupby = new GroupBy(pm.getKey(), new Fields(groupByFields.getFields()));
        groupby = new Every(groupby, buffer, Fields.RESULTS);

        return register(groupby, pm.getValue());
    }

    protected String addGroupByAndBuffer(String prior, FieldList groupByFields, Buffer buffer, List<FieldMetadata> fms) {
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pm = pipesAndOutputSchemas.get(prior);
        if (pm == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { prior });
        }
        Pipe groupby = null;
        groupby = new GroupBy(pm.getKey(), new Fields(groupByFields.getFields()));
        groupby = new Every(groupby, buffer, Fields.RESULTS);

        return register(groupby, fms);
    }

    protected String addGroupByAndBuffer(String prior, FieldList groupByFields, FieldList sortFields, Buffer buffer,
            boolean descending) {
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pm = pipesAndOutputSchemas.get(prior);
        if (pm == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { prior });
        }
        Pipe groupby = null;
        groupby = new GroupBy(pm.getKey(), new Fields(groupByFields.getFields()), new Fields(sortFields.getFields()),
                descending);
        groupby = new Every(groupby, buffer, Fields.RESULTS);

        return register(groupby, pm.getValue());
    }

    protected String addGroupByAndFirst(String[] priors, Fields groupByFields, Fields sortFields) {
        Pipe[] pipes = new Pipe[priors.length];
        List<FieldMetadata> fm = new ArrayList<>();
        for (int i = 0; i < priors.length; i++) {
            String prior = priors[i];
            AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pm = pipesAndOutputSchemas.get(prior);
            if (pm == null) {
                throw new LedpException(LedpCode.LEDP_26004, new String[] { prior });
            }
            pipes[i] = pm.getKey();
            fm = pm.getValue();
        }

        Pipe groupby = new GroupBy(pipes, groupByFields, sortFields);
        Pipe first = new Every(groupby, new First(), Fields.ARGS);
        return register(first, fm);
    }

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

    protected String addFunction(String prior, String expression, FieldList fieldsToApply, FieldMetadata targetField) {
        return addFunctionWithExpression(prior, expression, fieldsToApply, targetField, null);
    }

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
            fm = retainFields(outputFields, fm);
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

    protected String addRetain(String prior, FieldList outputFields) {
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pm = pipesAndOutputSchemas.get(prior);
        if (pm == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { prior });
        }

        Pipe retain = new Retain(pm.getKey(), convertToFields(outputFields.getFields()));

        List<FieldMetadata> fm = new ArrayList<>(pm.getValue());
        fm = retainFields(outputFields, fm);
        return register(retain, fm);
    }

    private List<FieldMetadata> retainFields(FieldList outputFields, List<FieldMetadata> fm) {
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

    protected String addDiscard(String prior, FieldList toDiscard) {
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pm = pipesAndOutputSchemas.get(prior);
        if (pm == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { prior });
        }

        Pipe discard = new Discard(pm.getKey(), convertToFields(toDiscard.getFields()));
        List<FieldMetadata> fm = new ArrayList<>(pm.getValue());
        discardFields(toDiscard, fm);
        return register(discard, fm);
    }

    private void discardFields(FieldList toDiscard, List<FieldMetadata> fm) {
        if (toDiscard != null) {
            for (String field : toDiscard.getFields()) {
                for (int i = 0; i < fm.size(); ++i) {
                    FieldMetadata fmentry = fm.get(i);
                    if (fmentry.getFieldName().equals(field)) {
                        fm.remove(i);
                        --i;
                    }
                }
            }
        }
    }

    protected String addRename(String prior, FieldList previousNames, FieldList newNames) {
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pm = pipesAndOutputSchemas.get(prior);
        if (pm == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { prior });
        }

        Pipe rename = new Rename(pm.getKey(), convertToFields(previousNames.getFields()),
                convertToFields(newNames.getFields()));
        List<FieldMetadata> fm = new ArrayList<>(pm.getValue());
        renameFields(previousNames, newNames, fm);
        return register(rename, fm);
    }

    private void renameFields(FieldList previousNames, FieldList newNames, List<FieldMetadata> metadata) {
        if (previousNames.getFields().length != newNames.getFields().length) {
            throw new RuntimeException("Previous and new name array lengths must be the same");
        }

        String[] previousNameStrings = previousNames.getFields();
        String[] newNameStrings = newNames.getFields();
        for (int i = 0; i < previousNameStrings.length; ++i) {
            String previousName = previousNameStrings[i];
            String newName = newNameStrings[i];

            boolean found = false;
            for (FieldMetadata field : metadata) {
                if (field.getFieldName().equals(previousName)) {
                    field.setFieldName(newName);
                    found = true;
                    break;
                }
            }

            if (!found) {
                throw new RuntimeException(String.format("Could not locate field with name %s in metadata",
                        previousName));
            }
        }
    }

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
        pdHashFm.setPropertyValue("displayName", "Prop Data Hash");
        newFm.add(pdHashFm);

        return register(each, newFm);
    }

    protected String addRowId(String prior, String targetFieldName) {
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pm = pipesAndOutputSchemas.get(prior);
        if (pm == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { prior });
        }
        Pipe each = new Each(pm.getKey(), Fields.ALL, new AddRowId(new Fields(targetFieldName), prior), Fields.ALL);
        List<FieldMetadata> newFm = new ArrayList<>(pm.getValue());
        FieldMetadata rowIdFm = new FieldMetadata(Type.LONG, Long.class, targetFieldName, null);
        rowIdFm.setPropertyValue("logicalType", "rowid");
        rowIdFm.setPropertyValue("displayName", "Row ID");
        newFm.add(rowIdFm);

        return register(each, newFm);
    }

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

    protected String addStopListFilter(String lhs, String rhs, String lhsJoinField, String rhsJoinField) {
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pmLhs = pipesAndOutputSchemas.get(lhs);
        if (pmLhs == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { lhs });
        }

        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pmRhs = pipesAndOutputSchemas.get(rhs);
        if (pmRhs == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { rhs });
        }

        String joined = addJoin(lhs, new FieldList(lhsJoinField), rhs, new FieldList(rhsJoinField), JoinType.OUTER);
        if (getFieldNames(pmLhs.getValue()).contains(rhsJoinField)) {
            rhsJoinField = joinFieldName(rhs, rhsJoinField);
        }
        String expression = String.format("%s == null", rhsJoinField);
        String filtered = addFilter(joined, expression, new FieldList(lhsJoinField, rhsJoinField));
        return filtered;
    }

    protected List<FieldMetadata> getMetadata(String identifier) {
        return pipesAndOutputSchemas.get(identifier).getValue();
    }

    @Override
    public Table runFlow(DataFlowContext dataFlowCtx) {
        reset();
        setDataFlowCtx(dataFlowCtx);

        @SuppressWarnings("unchecked")
        Map<String, String> sourcePaths = dataFlowCtx.getProperty("SOURCES", Map.class);
        @SuppressWarnings("unchecked")
        Map<String, Table> sourceTables = dataFlowCtx.getProperty("SOURCETABLES", Map.class);
        String flowName = dataFlowCtx.getProperty("FLOWNAME", String.class);
        String targetPath = dataFlowCtx.getProperty("TARGETPATH", String.class);
        Properties jobProperties = dataFlowCtx.getProperty("JOBPROPERTIES", Properties.class);
        String engineType = dataFlowCtx.getProperty("ENGINE", String.class);
        ExecutionEngine engine = ExecutionEngine.get(engineType);
        DataFlowParameters parameters = dataFlowCtx.getProperty("PARAMETERS", DataFlowParameters.class);

        String lastOperator = null;
        if (sourceTables != null) {
            lastOperator = constructFlowDefinition(parameters).getIdentifier();
        } else {
            lastOperator = constructFlowDefinition(dataFlowCtx, sourcePaths);
        }
        engine.setEnforceGlobalOrdering(enforceGlobalOrdering());

        Tap<?, ?, ?> sink = createSink(lastOperator, targetPath);

        Properties properties = new Properties();
        if (jobProperties != null) {
            properties.putAll(jobProperties);
        }
        AppProps.setApplicationJarClass(properties, getClass());
        FlowConnector flowConnector = engine.createFlowConnector(dataFlowCtx, properties);

        FlowDef flowDef = FlowDef.flowDef().setName(flowName + "_" + DateTime.now().getMillis()) //
                .addSources(getSources()) //
                .addTailSink(getPipeByIdentifier(lastOperator), sink);

        for (AbstractMap.SimpleEntry<Checkpoint, Tap> entry : checkpoints.values()) {
            flowDef = flowDef.addCheckpoint(entry.getKey(), entry.getValue());
        }
        DataFlowContext ctx = getDataFlowCtx();
        Configuration config = ctx.getProperty("HADOOPCONF", Configuration.class);

        log.info(String.format("About to run data flow %s using execution engine %s", flowName, engine.getName()));
        log.info("Using hadoop fs.defaultFS = " + config.get("fs.defaultFS"));
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
        return getTableMetadata(dataFlowCtx.getProperty("TARGETTABLENAME", String.class), //
                targetPath, //
                pipesAndOutputSchemas.get(lastOperator).getValue());
    }

    private Tap<?, ?, ?> createCheckpointSink(String name) {
        DataFlowContext ctx = getDataFlowCtx();
        String targetPath = String.format("/tmp/checkpoints/%s/%s/%s", //
                ctx.getProperty("CUSTOMER", String.class), //
                ctx.getProperty("FLOWNAME", String.class), //
                name);
        targetPath = getLocationPrefixedPath(targetPath);
        return new Hfs(new SequenceFile(Fields.UNKNOWN), targetPath, SinkMode.REPLACE);
    }

    private Tap<?, ?, ?> createTap(String sourcePath) {
        sourcePath = getLocationPrefixedPath(sourcePath);
        return new GlobHfs(new AvroScheme(), sourcePath);
    }

    private Tap<?, ?, ?> createSink(String lastOperator, String targetPath) {
        DataFlowContext context = getDataFlowCtx();
        String flowName = context.getProperty("FLOWNAME", String.class);
        Schema schema = getSchema(flowName, lastOperator, context);
        AvroScheme scheme = new AvroScheme(schema);
        if (enforceGlobalOrdering()) {
            scheme.setNumSinkParts(1);
        }

        targetPath = getLocationPrefixedPath(targetPath);

        Tap<?, ?, ?> sink = new Hfs(scheme, targetPath, SinkMode.REPLACE);
        return sink;
    }

    private String getLocationPrefixedPath(String path) {
        DataFlowContext context = getDataFlowCtx();
        Configuration configuration = context.getRequiredProperty("HADOOPCONF", Configuration.class);

        if (path.startsWith("file://")) {
            path = path.substring(7);
        } else if (path.startsWith("hdfs://")) {
            String[] parts = path.split("/");
            // skip over hdfs://hostname:port/
            List<String> partsSkipped = new ArrayList<>();
            for (int i = 3; i < parts.length; ++i) {
                partsSkipped.add(parts[i]);
            }
            path = Joiner.on("/").join(partsSkipped);
        }

        return configuration.get("fs.defaultFS") + path;
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
