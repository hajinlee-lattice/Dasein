package com.latticeengines.dataflow.runtime.cascading;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.domain.exposed.dataflow.operations.OperationLogUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings({ "rawtypes" })
public abstract class BaseGroupbyBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = -9177444191170437390L;
    protected Map<String, Integer> namePositionMap;
    protected boolean withOptLog = false;
    protected int logFieldIdx = -1;

    protected BaseGroupbyBuffer(Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
    }

    /**
     * If want to track operation log for buffer, please use this constructor by
     * passing withOptLog = true
     * 
     * @param fieldDeclaration
     * @param addOptLog:
     *            whether to add new column LE_OperationLogs in schema
     */
    public BaseGroupbyBuffer(Fields fieldDeclaration, boolean withOptLog) {
        super(withOptLog && !StreamSupport.stream(fieldDeclaration.spliterator(), false)
                .anyMatch(field -> OperationLogUtils.DEFAULT_FIELD_NAME.equals((String) field))
                        ? fieldDeclaration.append(new Fields(OperationLogUtils.DEFAULT_FIELD_NAME))
                        : fieldDeclaration);
        namePositionMap = getPositionMap(this.fieldDeclaration);
        this.withOptLog = namePositionMap.get(OperationLogUtils.DEFAULT_FIELD_NAME) != null;
        if (this.withOptLog) {
            logFieldIdx = namePositionMap.get(OperationLogUtils.DEFAULT_FIELD_NAME);
        }
    }

    protected Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        return IntStream.range(0, fieldDeclaration.size())
                .mapToObj(idx -> Pair.of((String) fieldDeclaration.get(idx), idx))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    @SuppressWarnings("unchecked")
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Tuple result = Tuple.size(getFieldDeclaration().size());
        TupleEntry group = bufferCall.getGroup();
        if (shouldSkipGroup(group)) {
            return;
        }
        if (shouldFlushGroup(group)) {
            flushGroup(bufferCall);
        } else {
            setupTupleForGroup(result, group);
            Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
            result = setupTupleForArgument(result, arguments);
            if (result != null) {
                bufferCall.getOutputCollector().add(result);
            }
        }
    }

    private void setupTupleForGroup(Tuple result, TupleEntry group) {
        Fields fields = group.getFields();
        for (Object field : fields) {
            String fieldName = (String) field;
            Integer loc = namePositionMap.get(fieldName);
            if (loc != null && loc >= 0) {
                result.set(loc, group.getObject(fieldName));
            } else {
                System.out.println("Warning: can not find field name=" + fieldName);
            }
        }
    }

    protected boolean shouldSkipGroup(TupleEntry group) {
        return false;
    }

    protected boolean shouldFlushGroup(TupleEntry group) {
        return false;
    }

    @SuppressWarnings("unchecked")
    private void flushGroup(BufferCall bufferCall) {
        Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
        while (iter.hasNext()) {
            bufferCall.getOutputCollector().add(iter.next());
        }
    }

    protected abstract Tuple setupTupleForArgument(Tuple result,
            Iterator<TupleEntry> argumentsInGroup);

}
