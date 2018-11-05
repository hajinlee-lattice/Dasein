package com.latticeengines.dataflow.runtime.cascading;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.domain.exposed.dataflow.operations.OperationLogUtils;

import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.tuple.Fields;

@SuppressWarnings("rawtypes")
public abstract class BaseFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = -9148414584937102317L;

    protected Map<String, Integer> namePositionMap;
    protected boolean withOptLog = false;
    protected int logFieldIdx = -1;

    protected BaseFunction(Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
    }

    /**
     * If want to track operation log for function, please use this constructor
     * by passing withOptLog = true
     * 
     * @param fieldDeclaration
     * @param addOptLog:
     *            whether to add new column LE_OperationLogs in schema
     */
    public BaseFunction(Fields fieldDeclaration, boolean withOptLog) {
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
}
