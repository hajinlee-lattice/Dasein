package com.latticeengines.dataflow.exposed.builder;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.commons.collections4.comparators.NullComparator;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tez.dag.api.TezConfiguration;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.hadoop.fs.HdfsResourceLoader;
import org.springframework.retry.support.RetryTemplate;

import com.google.common.collect.Lists;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.dataflow.exception.RetryableFlowException;
import com.latticeengines.dataflow.exposed.builder.common.Aggregation;
import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.exposed.builder.operations.DebugOperation;
import com.latticeengines.dataflow.exposed.builder.operations.FunctionOperation;
import com.latticeengines.dataflow.exposed.builder.operations.Operation;
import com.latticeengines.dataflow.exposed.builder.util.DataFlowUtils;
import com.latticeengines.dataflow.runtime.cascading.AddMD5Hash;
import com.latticeengines.dataflow.runtime.cascading.AddNullColumns;
import com.latticeengines.dataflow.runtime.cascading.AddRowIdFunction;
import com.latticeengines.dataflow.runtime.cascading.GroupAndExpandFieldsBuffer;
import com.latticeengines.dataflow.service.impl.listener.DataFlowListener;
import com.latticeengines.dataflow.service.impl.listener.DataFlowStepListener;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.dataflow.ExtractFilter;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;

import cascading.avro.AvroScheme;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowException;
import cascading.flow.FlowStep;
import cascading.flow.StepCounters;
import cascading.operation.NoOp;
import cascading.operation.aggregator.First;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.filter.Not;
import cascading.pipe.Checkpoint;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
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

@SuppressWarnings("rawtypes")
public abstract class CascadingDataFlowBuilder extends DataFlowBuilder {

    private static final Logger log = LoggerFactory.getLogger(CascadingDataFlowBuilder.class);

    private Integer counter = 1;

    private Map<String, Tap> taps = new HashMap<>();

    private Map<String, AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>>> pipesAndOutputSchemas = new HashMap<>();

    private Map<String, AbstractMap.SimpleEntry<Checkpoint, Tap>> checkpoints = new HashMap<>();

    private DataFlowListener dataFlowListener = new DataFlowListener();

    private DataFlowStepListener dataFlowStepListener = new DataFlowStepListener();

    String lastOperator = null;

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

    private String register(Pipe pipe, List<FieldMetadata> fields) {
        String nodename = register(pipe, fields, null);
        return addDebugInfo(nodename, pipe, fields);
    }

    private String addDebugInfo(String prior, Pipe pipe, List<FieldMetadata> fields) {
        if (isDebug()) {
            StackTraceElement[] trace = Thread.currentThread().getStackTrace();
            String prefix = "undefined";
            for (StackTraceElement elt : trace) {
                if (elt.getClassName().equals(Node.class.getName())) {
                    prefix = String.format("%s", elt.getMethodName(), elt.getFileName(), elt.getLineNumber());
                    break;
                }
            }

            // Search for TypesafeDataFlowBuilder implementation
            List<StackTraceElement> reversed = Lists.reverse(Arrays.asList(trace));
            int fetchAt = -1;
            StackTraceElement builderStackElt = null;
            for (int i = 0; i < reversed.size(); ++i) {
                StackTraceElement elt = reversed.get(i);
                if (fetchAt == i) {
                    builderStackElt = elt;
                    break;
                }
                if (elt.getClassName().equals(TypesafeDataFlowBuilder.class.getName())) {
                    fetchAt = i + 2;
                }
            }
            if (builderStackElt != null) {
                prefix += String.format(" (%s:%d)", builderStackElt.getFileName(), builderStackElt.getLineNumber());
            }

            Operation debug = new DebugOperation(new Operation.Input(pipe, fields), prefix, 1);
            return register(debug.getOutputPipe(), debug.getOutputMetadata(), null);
        }
        return prior;
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
        Map<String, Field> allColumns = new LinkedHashMap<>();
        return addSource(sourceTableName, allColumns);
    }

    @SuppressWarnings("unchecked")
    protected Node addSource(String sourceTableName, Map<String, Field> allColumns) {
        DataFlowContext ctx = getDataFlowCtx();
        Map sourceTables = ctx.getProperty(DataFlowProperty.SOURCETABLES, Map.class);
        Table sourceTable = (Table) sourceTables.get(sourceTableName);
        if (sourceTable == null) {
            throw new RuntimeException(String.format("Could not find source with name %s", sourceTableName));
        }
        validateTableForSource(sourceTable);

        Configuration config = ctx.getProperty(DataFlowProperty.HADOOPCONF, Configuration.class);

        List<Extract> extracts = filterExtracts(sourceTable.getName(), sourceTable.getExtracts());

        Schema[] allSchemas = new Schema[extracts.size()];
        int i = 0;
        for (Extract extract : extracts) {

            String path = null;
            try {
                log.info(String.format("Retrieving extract for table %s located at %s", sourceTableName,
                        extract.getPath()));
                List<String> matches = HdfsUtils.getFilesByGlob(config, extract.getPath());
                if (HdfsUtils.isDirectory(config, extract.getPath())) {
                    matches = HdfsUtils.getFilesByGlob(config, extract.getPath() + "/*.avro");
                }

                if (matches.size() == 0) {
                    throw new IllegalStateException(
                            String.format("Could not find extract with path %s in HDFS", extract.getPath()));
                }
                path = matches.get(0);
                allSchemas[i] = AvroUtils.getSchema(config, new Path(path));
                for (Field field : allSchemas[i].getFields()) {
                    allColumns.put(field.name(), field);
                }
                i++;
            } catch (Exception e) {
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

        Set<String> existing = new HashSet<>();
        for (Extract extract : extracts) {
            Set<String> allColumnsClone = new HashSet<>(allColumns.keySet());
            for (Field field : allSchemas[i].getFields()) {
                allColumnsClone.remove(field.name());
            }
            String sourceName = String.format("%s-%s", sourceTableName, extract.getName());
            if (existing.contains(sourceName)) {
                String newName = sourceName + "_" + UUID.randomUUID();
                log.warn(String.format("Changing source %s to %s to avoid collision with existing source", sourceName,
                        newName));
                sourceName = newName;
            }
            String extractPath = extract.getPath();
            if (!extractPath.endsWith(".avro")) {
                if (!extractPath.endsWith("/")) {
                    extractPath += "/";
                }
                extractPath += "*.avro";
            }
            String source = addSource(sourceName, extractPath, true);
            String[] extraCols = new String[allColumnsClone.size()];
            allColumnsClone.toArray(extraCols);

            if (allColumnsClone.size() > 0) {
                pipes[i] = new Each(new Pipe(source), new AddNullColumns(new Fields(extraCols)), declaredFields);
            } else {
                pipes[i] = new Each(new Pipe(source), new NoOp(), declaredFields);
            }
            i++;
            existing.add(sourceName);
        }

        for (Pipe pipe : pipes) {
            log.info("Pipe name = " + pipe.getName());
        }

        Pipe toRegister = new Merge(pipes);

        // group and sort the extracts
        if (sourceTable.getPrimaryKey() != null && sourceTable.getLastModifiedKey() != null) {
            log.info(String.format("Deduping %s by %s using %s to resolve ties", sourceTable.getName(), //
                    sourceTable.getPrimaryKey().getAttributes().get(0), //
                    sourceTable.getLastModifiedKey().getAttributes().get(0)));
            String lastModifiedKeyColName = sourceTable.getLastModifiedKey().getAttributes().get(0);
            Fields sortFields = new Fields(lastModifiedKeyColName);
            sortFields.setComparator(lastModifiedKeyColName, new NullComparator(Collections.reverseOrder()));

            Pipe groupby = new GroupBy(toRegister, new Fields(sourceTable.getPrimaryKey().getAttributeNames()),
                    sortFields);
            toRegister = new Every(groupby, Fields.ALL, new First(), Fields.RESULTS);
        }
        toRegister = new Pipe(sourceTableName, toRegister);

        List<FieldMetadata> fm = getFieldMetadata(allColumns, sourceTable);
        return new Node(register(toRegister, fm, sourceTableName), this);
    }

    protected List<String> getSourceFiles(String sourceTableName) {
        DataFlowContext ctx = getDataFlowCtx();
        Map sourceTables = ctx.getProperty(DataFlowProperty.SOURCETABLES, Map.class);
        Table sourceTable = (Table) sourceTables.get(sourceTableName);
        if (sourceTable == null) {
            throw new RuntimeException(String.format("Could not find source with name %s", sourceTableName));
        }
        validateTableForSource(sourceTable);

        Configuration config = ctx.getProperty(DataFlowProperty.HADOOPCONF, Configuration.class);

        List<Extract> extracts = filterExtracts(sourceTable.getName(), sourceTable.getExtracts());

        for (Extract extract : extracts) {

            try {
                log.info(String.format("Retrieving extract for table %s located at %s", sourceTableName,
                        extract.getPath()));
                List<String> matches = HdfsUtils.getFilesByGlob(config, extract.getPath());
                if (HdfsUtils.isDirectory(config, extract.getPath())) {
                    matches = HdfsUtils.getFilesByGlob(config, extract.getPath() + "/*.avro");
                }

                if (matches.size() == 0) {
                    throw new IllegalStateException(
                            String.format("Could not find extract with path %s in HDFS", extract.getPath()));
                }

                return matches;
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_26005, e);
            }
        }

        return null;
    }

    protected Table getSourceMetadata(String sourceName) {
        DataFlowContext ctx = getDataFlowCtx();
        @SuppressWarnings("unchecked")
        Map<String, Table> sourceTables = ctx.getProperty(DataFlowProperty.SOURCETABLES, Map.class);
        Table retrieved = sourceTables.get(sourceName);
        if (retrieved == null) {
            throw new RuntimeException(String.format("%s is not a valid source", sourceName));
        }
        return retrieved;
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

        if (sourceTable.getPrimaryKey() != null) {
            if (sourceTable.getPrimaryKey().getAttributes().size() == 0) {
                throw new LedpException(LedpCode.LEDP_26008, new String[] { sourceTable.getName() });
            }
        }
    }

    protected String addSource(String sourceName, String sourcePath, List<FieldMetadata> fields) {
        return addSource(sourceName, sourcePath, true, fields);
    }

    protected String addSource(String sourceName, String sourcePath) {
        return addSource(sourceName, sourcePath, true, null);
    }

    protected String addSource(String sourceName, String sourcePath, boolean regex) {
        return addSource(sourceName, sourcePath, regex, null);
    }

    private String addSource(String sourceName, String sourcePath, boolean regex, List<FieldMetadata> fields) {
        Tap<?, ?, ?> tap = createTap(sourcePath);

        taps.put(sourceName, tap);

        Configuration config = getConfig();
        if (fields == null) {
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
            fields = getMetadataFromSchema(sourceSchema);
        }

        return register(new Pipe(sourceName), fields, sourceName);
    }

    private List<FieldMetadata> getMetadataFromSchema(Schema sourceSchema) {
        List<FieldMetadata> fields;
        fields = new ArrayList<>(sourceSchema.getFields().size());
        for (Field field : sourceSchema.getFields()) {
            Type avroType = AvroUtils.getType(field);
            FieldMetadata fm = new FieldMetadata(avroType, AvroUtils.getJavaType(avroType), field.name(), field);
            fields.add(fm);
        }
        return fields;
    }

    @SuppressWarnings("unchecked")
    private List<Extract> filterExtracts(String sourceTableName, List<Extract> original) {
        Map extractFilters = getDataFlowCtx().getProperty(DataFlowProperty.EXTRACTFILTERS, Map.class);

        List<Extract> filtered = new ArrayList<>();
        for (Extract extract : original) {
            boolean allowed = true;

            if (extractFilters != null) {
                List<ExtractFilter> filters = (List<ExtractFilter>) extractFilters.get(sourceTableName);
                if (filters != null) {
                    for (ExtractFilter filter : filters) {
                        allowed = allowed && filter.allows(extract);
                    }
                }
            }

            if (allowed) {
                filtered.add(extract);
            }
        }

        return filtered;
    }

    protected String joinFieldName(String identifier, String fieldName) {
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> lookup = pipesAndOutputSchemas.get(identifier);
        if (lookup == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { identifier });
        }
        return lookup.getKey().getName().replaceAll("\\*|-", "__") + "__" + fieldName;
    }

    protected Configuration getConfig() {
        DataFlowContext ctx = getDataFlowCtx();
        Configuration config = ctx.getProperty(DataFlowProperty.HADOOPCONF, Configuration.class);
        if (config == null) {
            config = new Configuration();
        }
        return config;
    }

    private String getSchemaPath(Configuration config, String sourcePath) throws Exception {
        if (HdfsUtils.isDirectory(config, sourcePath)) {
            sourcePath = sourcePath + "/*.avro";
        }
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

    protected String addJoin(String lhs, FieldList lhsJoinFields, String rhs, FieldList rhsJoinFields,
            JoinType joinType) {
        return internalAddJoin(lhs, lhsJoinFields, rhs, rhsJoinFields, joinType, false);
    }

    private String internalAddJoin(String lhs, FieldList lhsJoinFields, String rhs, FieldList rhsJoinFields,
            JoinType joinType, boolean hashJoin) {
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> lhsPipesAndFields = pipesAndOutputSchemas.get(lhs);
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> rhsPipesAndFields = pipesAndOutputSchemas.get(rhs);
        if (lhsPipesAndFields == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { lhs });
        }
        if (rhsPipesAndFields == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { rhs });
        }
        List<FieldMetadata> declaredFields = combineFields(rhs, lhsPipesAndFields.getValue(),
                rhsPipesAndFields.getValue());

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

        Pipe join = null;
        if (!hashJoin) {
            join = new CoGroup(lhsPipesAndFields.getKey(), //
                    DataFlowUtils.convertToFields(lhsJoinFields.getFields()), //
                    rhsPipesAndFields.getKey(), //
                    DataFlowUtils.convertToFields(rhsJoinFields.getFields()), //
                    DataFlowUtils.convertToFields(DataFlowUtils.getFieldNames(declaredFields)), //
                    joiner);
        } else {
            join = new HashJoin(lhsPipesAndFields.getKey(), //
                    DataFlowUtils.convertToFields(lhsJoinFields.getFields()), //
                    rhsPipesAndFields.getKey(), //
                    DataFlowUtils.convertToFields(rhsJoinFields.getFields()), //
                    DataFlowUtils.convertToFields(DataFlowUtils.getFieldNames(declaredFields)), //
                    joiner);
        }
        return register(join, declaredFields);
    }

    protected String addCoGroup(List<String> identifiers, List<FieldList> groupFieldLists, JoinType joinType) {

        if (identifiers.size() != groupFieldLists.size()) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { "Group size mismatch" });
        }

        if (identifiers.size() == 0) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { "No id defined" });
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

        int groupSize = identifiers.size();
        Pipe[] pipes = new Pipe[groupSize];
        Fields[] groupFieldsArray = new Fields[groupSize];
        List<FieldMetadata> declaredFields = null;

        for (int i = 0; i < groupSize; i++) {

            String id = identifiers.get(i);
            FieldList fieldList = groupFieldLists.get(i);

            AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pipesAndFields = pipesAndOutputSchemas.get(id);
            List<FieldMetadata> metadataList = pipesAndFields.getValue();
            if (declaredFields == null) {
                declaredFields = metadataList;
            } else {
                declaredFields = combineFields(id, declaredFields, metadataList);
            }

            pipes[i] = pipesAndFields.getKey();
            groupFieldsArray[i] = DataFlowUtils.convertToFields(fieldList.getFields());
        }

        Pipe coGroup = new CoGroup(pipes, groupFieldsArray, //
                DataFlowUtils.convertToFields(DataFlowUtils.getFieldNames(declaredFields)), //
                joiner);
        return register(coGroup, declaredFields);
    }

    private List<FieldMetadata> combineFields(String rhsId, List<FieldMetadata> lhsFields,
            List<FieldMetadata> rhsFields) {
        List<FieldMetadata> declaredFields = new ArrayList<>();
        Set<String> seenFields = new HashSet<>();

        List<String> outputFields = new ArrayList<>();
        outputFields.addAll(DataFlowUtils.getFieldNames(lhsFields));
        Map<String, FieldMetadata> nameToFieldMetadataMap = DataFlowUtils.getFieldMetadataMap(lhsFields);
        for (String fieldName : outputFields) {
            seenFields.add(fieldName);
            declaredFields.add(nameToFieldMetadataMap.get(fieldName));
        }

        outputFields = new ArrayList<>();
        outputFields.addAll(DataFlowUtils.getFieldNames(rhsFields));
        nameToFieldMetadataMap = DataFlowUtils.getFieldMetadataMap(rhsFields);
        for (String fieldName : outputFields) {
            String originalFieldName = fieldName;

            if (seenFields.contains(fieldName)) {
                fieldName = joinFieldName(rhsId, fieldName);
                if (seenFields.contains(fieldName)) {
                    throw new RuntimeException(String.format(
                            "Cannot create joinFieldName %s from field name %s because a field with that name already exists.  Discard the field to avoid this error",
                            fieldName, originalFieldName));
                }
            }
            seenFields.add(fieldName);
            FieldMetadata origfm = nameToFieldMetadataMap.get(originalFieldName);
            FieldMetadata fm = new FieldMetadata(origfm.getAvroType(), origfm.getJavaType(), fieldName,
                    origfm.getField(), origfm.getProperties(), null);
            declaredFields.add(fm);
        }
        return declaredFields;
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

    public Schema getSchemaFromFile(DataFlowContext dataFlowCtx) {
        String targetSchemaPath = dataFlowCtx.getProperty("TARGETSCHEMAPATH", String.class);
        if (targetSchemaPath != null) {
            return getSchemaFromFilePath(targetSchemaPath);
        }
        return null;
    }

    protected Schema getSchemaFromFilePath(String schemaPath) {
        Configuration config = getConfig();
        try {
            return AvroUtils.getSchema(config, new Path(getSchemaPath(config, schemaPath)));
        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_26005, ex);
        }
    }

    protected FieldList buildFieldListFromSchema(String schemaPath) {
        try (HdfsResourceLoader resourceLoader = new HdfsResourceLoader(FileSystem.newInstance(getConfig()))) {
            Schema schema = AvroUtils.readSchemaFromResource(resourceLoader, schemaPath);
            List<String> fieldNames = new ArrayList<>();
            for (Field field : schema.getFields()) {
                fieldNames.add(field.name());
            }
            return new FieldList(fieldNames.toArray(new String[0]));
        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_26005, ex);
        }
    }

    protected List<FieldMetadata> getMetadataFromSchemaPath(String schemaPath) {
        try (HdfsResourceLoader resourceLoader = new HdfsResourceLoader(FileSystem.newInstance(getConfig()))) {
            Schema schema = AvroUtils.readSchemaFromResource(resourceLoader, schemaPath);
            return getMetadataFromSchema(schema);
        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_26005, ex);
        }
    }

    public String addGroupBy(String prior, FieldList groupByFieldList, List<Aggregation> aggregation) {
        return addGroupBy(prior, groupByFieldList, null, aggregation);
    }

    public String addGroupBy(String prior, FieldList groupByFieldList, FieldList sortFieldList,
            List<Aggregation> aggregations) {
        List<String> groupByFields = groupByFieldList.getFieldsAsList();
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pm = pipesAndOutputSchemas.get(prior);
        if (pm == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { prior });
        }
        Map<String, FieldMetadata> nameToFieldMetadataMap = DataFlowUtils.getFieldMetadataMap(pm.getValue());

        Pipe groupby = null;

        if (sortFieldList != null) {
            groupby = new GroupBy(pm.getKey(), DataFlowUtils.convertToFields(groupByFields),
                    DataFlowUtils.convertToFields(sortFieldList.getFieldsAsList()));
        } else {
            groupby = new GroupBy(pm.getKey(), DataFlowUtils.convertToFields(groupByFields));
        }

        List<FieldMetadata> declaredFields = DataFlowUtils.getIntersection(groupByFields,
                pipesAndOutputSchemas.get(prior).getValue());

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
                case NONE:
                    outputStrategy = Fields.NONE;
                default:
                    break;
                }
            }
            groupby = new Every(groupby,
                    aggFieldName == null ? Fields.ALL : DataFlowUtils.convertToFields(aggFieldName), //
                    DataFlowUtils.getAggregator(aggregation.getTargetFieldName(), aggregation.getAggregationType()), //
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
    public String addGroupByAndExpand(String prior, FieldList groupByFieldList, String expandField,
            List<String> expandFormats, FieldList argumentsFieldList, FieldList declaredFieldList) {
        List<String> groupByFields = groupByFieldList.getFieldsAsList();
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pm = pipesAndOutputSchemas.get(prior);
        if (pm == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { prior });
        }
        Pipe groupby = null;
        groupby = new GroupBy(pm.getKey(), DataFlowUtils.convertToFields(groupByFields));
        groupby = new Every(groupby,
                argumentsFieldList == null ? Fields.ALL
                        : DataFlowUtils.convertToFields(argumentsFieldList.getFieldsAsList()), //
                new GroupAndExpandFieldsBuffer(argumentsFieldList.getFieldsAsList().size(), expandField, expandFormats,
                        DataFlowUtils.convertToFields(declaredFieldList.getFieldsAsList())),
                Fields.RESULTS);

        List<FieldMetadata> fieldMetadata = new ArrayList<FieldMetadata>();

        return register(groupby, fieldMetadata);
    }

    public String addFilter(String prior, String expression, FieldList filterFieldList) {
        List<String> filterFields = filterFieldList.getFieldsAsList();
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pm = pipesAndOutputSchemas.get(prior);
        String[] filterFieldsArray = new String[filterFields.size()];
        filterFields.toArray(filterFieldsArray);
        Not filter = new Not(new ExpressionFilter(expression, filterFieldsArray,
                DataFlowUtils.getTypes(filterFields, pm.getValue())));
        Pipe each = new Each(pm.getKey(), DataFlowUtils.convertToFields(filterFields), filter);
        List<FieldMetadata> fm = new ArrayList<>(pm.getValue());
        return register(each, fm);
    }

    @Deprecated
    public String addFunction(String prior, String expression, FieldList fieldsToApply, FieldMetadata targetField) {
        return register(new FunctionOperation(opInput(prior), expression, fieldsToApply, targetField));
    }

    @Deprecated
    public String addFunction(String prior, String expression, FieldList fieldsToApply, FieldMetadata targetField,
            FieldList outputFields) {
        return register(new FunctionOperation(opInput(prior), expression, fieldsToApply, targetField, outputFields));
    }

    public String addRetain(String prior, FieldList outputFields) {
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pm = pipesAndOutputSchemas.get(prior);
        if (pm == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { prior });
        }

        Pipe retain = new Retain(pm.getKey(), DataFlowUtils.convertToFields(outputFields.getFields()));

        List<FieldMetadata> fm = new ArrayList<>(pm.getValue());
        fm = DataFlowUtils.retainOnlyTheseFields(outputFields, fm);
        return register(retain, fm);
    }

    public String addDiscard(String prior, FieldList toDiscard) {
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pm = pipesAndOutputSchemas.get(prior);
        if (pm == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { prior });
        }

        Pipe discard = new Discard(pm.getKey(), DataFlowUtils.convertToFields(toDiscard.getFields()));
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

    public String addMD5(String prior, FieldList fieldsToApply, String targetFieldName) {
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pm = pipesAndOutputSchemas.get(prior);
        if (pm == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { prior });
        }
        Pipe each = new Each(pm.getKey(), DataFlowUtils.convertToFields(fieldsToApply.getFields()),
                new AddMD5Hash(new Fields(targetFieldName), null), Fields.ALL);
        List<FieldMetadata> newFm = new ArrayList<>(pm.getValue());
        FieldMetadata pdHashFm = new FieldMetadata(Type.STRING, String.class, targetFieldName, null);
        pdHashFm.setPropertyValue("length", "32");
        pdHashFm.setPropertyValue("displayName", "Prop Data Hash");
        newFm.add(pdHashFm);

        return register(each, newFm);
    }

    public String addCombine(String lhs, String rhs) {
        lhs = addRowId(lhs, "__rowid__");
        rhs = addRowId(rhs, "__rowid__");
        String last = addJoin(lhs, new FieldList("__rowid__"), rhs, new FieldList("__rowid__"), JoinType.OUTER);
        return addDiscard(last, new FieldList("__rowid__", joinFieldName(rhs, "__rowid__")));
    }

    public String addRowId(String prior, String targetFieldName) {
        FieldMetadata rowIdFm = new FieldMetadata(Type.LONG, Long.class, targetFieldName, null);
        rowIdFm.setPropertyValue("logicalType", LogicalDataType.RowId.toString());
        rowIdFm.setPropertyValue("displayName", "Row ID");

        return addRowId(prior, rowIdFm);
    }

    public String addRowId(String prior, FieldMetadata rowIdFm) {
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pm = pipesAndOutputSchemas.get(prior);
        if (pm == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { prior });
        }
        Pipe each = new Each(pm.getKey(), Fields.ALL, new AddRowIdFunction(new Fields(rowIdFm.getFieldName()), prior),
                Fields.ALL);
        List<FieldMetadata> newFm = new ArrayList<>(pm.getValue());
        newFm.add(rowIdFm);

        return register(each, newFm);
    }

    public String addStopListFilter(String lhs, String rhs, String lhsJoinField, String rhsJoinField) {
        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pmLhs = pipesAndOutputSchemas.get(lhs);
        if (pmLhs == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { lhs });
        }

        AbstractMap.SimpleEntry<Pipe, List<FieldMetadata>> pmRhs = pipesAndOutputSchemas.get(rhs);
        if (pmRhs == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { rhs });
        }

        String joined = addJoin(lhs, new FieldList(lhsJoinField), rhs, new FieldList(rhsJoinField), JoinType.OUTER);
        if (DataFlowUtils.getFieldNames(pmLhs.getValue()).contains(rhsJoinField)) {
            rhsJoinField = joinFieldName(rhs, rhsJoinField);
        }
        String expression = String.format("%s == null", rhsJoinField);
        String filtered = addFilter(joined, expression, new FieldList(lhsJoinField, rhsJoinField));
        return filtered;
    }

    public List<FieldMetadata> getMetadata(String identifier) {
        return pipesAndOutputSchemas.get(identifier).getValue();
    }

    public void setMetadata(String identifier, List<FieldMetadata> fms) {
        pipesAndOutputSchemas.get(identifier).setValue(fms);
    }

    public FieldMetadata getMetadata(String identifier, String fieldName) {
        List<FieldMetadata> metadata = getMetadata(identifier);
        Map<String, FieldMetadata> map = DataFlowUtils.getFieldMetadataMap(metadata);
        return map.get(fieldName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Table runFlow(DataFlowContext dataFlowCtx, String artifactVersion) {
        reset();
        setDataFlowCtx(dataFlowCtx);

        Map sourcePaths = dataFlowCtx.getProperty(DataFlowProperty.SOURCES, Map.class);
        Map sourceTables = dataFlowCtx.getProperty(DataFlowProperty.SOURCETABLES, Map.class);
        String flowName = dataFlowCtx.getProperty(DataFlowProperty.FLOWNAME, String.class);
        String targetPath = dataFlowCtx.getProperty(DataFlowProperty.TARGETPATH, String.class);
        Properties jobProperties = dataFlowCtx.getProperty(DataFlowProperty.JOBPROPERTIES, Properties.class);
        String engineType = dataFlowCtx.getProperty(DataFlowProperty.ENGINE, String.class);
        ExecutionEngine engine = ExecutionEngine.get(engineType);
        DataFlowParameters parameters = dataFlowCtx.getProperty(DataFlowProperty.PARAMETERS, DataFlowParameters.class);

        if ("FLINK".equalsIgnoreCase(engineType) && (parameters != null && parameters.noFlink)) {
            engine = ExecutionEngine.get("TEZ");
            log.info("This dataflow cannot use flink engine, fall back to tez.");
        }

        if (sourceTables != null) {
            lastOperator = constructFlowDefinition(parameters).getIdentifier();
        } else {
            lastOperator = constructFlowDefinition(dataFlowCtx, sourcePaths);
        }
        engine.setEnforceGlobalOrdering(enforceGlobalOrdering());

        FlowDef flowDef = constructFlowDef();
        DataFlowContext ctx = getDataFlowCtx();
        Configuration config = ctx.getProperty(DataFlowProperty.HADOOPCONF, Configuration.class);
        Properties properties = new Properties();

        log.info(String.format("About to run data flow %s using execution engine %s", flowName, engine.getName()));
        log.info("Using hadoop fs.defaultFS = " + config.get(FileSystem.FS_DEFAULT_NAME_KEY));
        String appJarPath = "";
        try {
            String dataFlowLibDir = StringUtils.isEmpty(artifactVersion) ? "/app/dataflow/lib/"
                    : "/app/" + artifactVersion + "/dataflow/lib/";
            log.info("Using dataflow lib path = " + dataFlowLibDir);
            List<String> files = HdfsUtils.getFilesForDir(config, dataFlowLibDir);
            for (String file : files) {
                String jarId = file.substring(file.lastIndexOf("/"));
                if (jarId.contains("le-dataflow-")) {
                    appJarPath = file;
                } else {
                    log.info("Adding " + file + " to flowdef classpath.");
                    flowDef.addToClassPath(file);
                }
            }
        } catch (Exception e) {
            log.warn("Exception retrieving library jars for this flow.");
        }

        try {
            TezConfiguration localConfig = new TezConfiguration();
            for (Map.Entry<String, String> entry : localConfig) {
                if (entry.getKey().toLowerCase().contains("tez")) {
                    log.info("[TEZ] " + entry.getKey() + " : " + entry.getValue());
                }
            }
        } catch (Exception e) {
            // ignore
        }

        if (jobProperties != null) {
            properties.putAll(jobProperties);
        }

        AppProps.setApplicationJarClass(properties, getClass());
        if (StringUtils.isNotBlank(appJarPath)) {
            log.info("Set application jar path to " + appJarPath);
            AppProps.setApplicationJarPath(properties, appJarPath);
        }

        RetryTemplate retry = RetryUtils.getRetryTemplate(3, //
                Collections.singleton(RetryableFlowException.class), Collections.emptyList());
        final ExecutionEngine finalEngine = engine;
        Flow<?> flow = retry.execute(retryContext -> {
            if (retryContext.getRetryCount() > 0) {
                log.info("(Attempt=" + retryContext.getRetryCount() + ") retry cascading flow.");
            }
            try {
                FlowConnector flowConnector = finalEngine.createFlowConnector(dataFlowCtx, properties);
                Flow<?> flow0 = flowConnector.connect(flowDef);
                flow0.writeDOT("dot/wcr.dot");
                flow0.addListener(dataFlowListener);
                flow0.addStepListener(dataFlowStepListener);
                flow0.complete();
                return flow0;
            } catch (FlowException e) {
                String stackTrace = ExceptionUtils.getStackTrace(e);
                // Shuffle failed with too many fetch failures and insufficient progress!
                if (stackTrace.contains("too many fetch failures")) {
                    log.warn("Too many fetch failures error", e);
                    throw new RetryableFlowException(e);
                } else {
                    throw e;
                }
            }
        });

        // CascadeConnector connector = new CascadeConnector();
        // Cascade cascade = connector.connect(flow);
        // cascade.complete();

        List steps = flow.getFlowSteps();
        Long tuplesWritten = null;
        try {
            if (steps != null && !steps.isEmpty()) {
                FlowStep lastStep = (FlowStep) steps.get(steps.size() - 1);
                tuplesWritten = lastStep.getFlowStepStats().getCounterValue(StepCounters.Tuples_Written);
                log.info("Wrote " + tuplesWritten + " in last step.");
            }
        } catch (Exception e) {
            log.error("Failed to read final count", e);
        }
        Table resultTable = getTableMetadata(
                dataFlowCtx.getProperty(DataFlowProperty.TARGETTABLENAME, String.class), //
                targetPath, //
                pipesAndOutputSchemas.get(lastOperator).getValue());
        if (tuplesWritten != null && tuplesWritten > 0) {
            resultTable.setCount(tuplesWritten);
        }
        return resultTable;
    }

    protected FlowDef constructFlowDef() {
        String flowName = dataFlowCtx.getProperty(DataFlowProperty.FLOWNAME, String.class);
        String targetPath = dataFlowCtx.getProperty(DataFlowProperty.TARGETPATH, String.class);
        Tap<?, ?, ?> sink = createSink(lastOperator, targetPath);

        FlowDef flowDef = FlowDef.flowDef().setName(flowName + "_" + DateTime.now().getMillis()) //
                .addSources(getSources()) //
                .addTailSink(getPipeByIdentifier(lastOperator), sink);

        for (AbstractMap.SimpleEntry<Checkpoint, Tap> entry : checkpoints.values()) {
            flowDef = flowDef.addCheckpoint(entry.getKey(), entry.getValue());
        }

        return flowDef;
    }

    private Tap<?, ?, ?> createCheckpointSink(String name) {
        DataFlowContext ctx = getDataFlowCtx();
        String targetPath = String.format("/tmp/checkpoints/%s/%s/%s", //
                ctx.getProperty(DataFlowProperty.CUSTOMER, String.class), //
                ctx.getProperty(DataFlowProperty.FLOWNAME, String.class), //
                name);
        targetPath = DataFlowUtils.getLocationPrefixedPath(this, targetPath);
        return new Hfs(new SequenceFile(Fields.UNKNOWN), targetPath, SinkMode.REPLACE);
    }

    private Tap<?, ?, ?> createTap(String sourcePath) {
        sourcePath = DataFlowUtils.getLocationPrefixedPath(this, sourcePath);
        return new GlobHfs(new AvroScheme(), sourcePath);
    }

    protected Tap<?, ?, ?> createSink(String lastOperator, String targetPath) {
        DataFlowContext context = getDataFlowCtx();
        String flowName = context.getProperty(DataFlowProperty.FLOWNAME, String.class);
        Schema schema = getSchema(flowName, lastOperator, context);
        AvroScheme scheme = new AvroScheme(schema);
        if (enforceGlobalOrdering()) {
            scheme.setNumSinkParts(1);
        }

        targetPath = DataFlowUtils.getLocationPrefixedPath(this, targetPath);

        Tap<?, ?, ?> sink = new Hfs(scheme, targetPath, SinkMode.REPLACE);
        return sink;
    }

    private Operation.Input opInput(String identifier) {
        Map.Entry<Pipe, List<FieldMetadata>> pipeAndMetadata = getPipeAndMetadata(identifier);
        if (pipeAndMetadata == null) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { identifier });
        }
        return new Operation.Input(pipeAndMetadata.getKey(), Lists.newArrayList(pipeAndMetadata.getValue()));
    }

    protected String addHashJoin(String lhs, FieldList lhsJoinFields, String rhs, FieldList rhsJoinFields,
            JoinType joinType) {
        return internalAddJoin(lhs, lhsJoinFields, rhs, rhsJoinFields, joinType, true);
    }
}
