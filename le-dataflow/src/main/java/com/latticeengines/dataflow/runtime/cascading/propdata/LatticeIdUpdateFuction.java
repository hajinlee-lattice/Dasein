package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings({ "rawtypes", "serial" })
public class LatticeIdUpdateFuction extends BaseOperation implements Function {
    private static final String ACTIVE = "ACTIVE";
    private static final String UPDATED = "UPDATED";
    private Map<String, Integer> namePositionMap;
    private String status;
    private String statusField;
    private int statusLoc;
    private int timestampLoc;
    private String copyIdFrom;
    private List<String> copyIdTo;
    private List<String> idsKeys;
    private List<String> entityKeys;

    public LatticeIdUpdateFuction(Fields fieldDeclaration, String status, String statusField,
            String timestampField, String copyIdFrom, List<String> copyIdTo, List<String> idsKeys,
            List<String> entityKeys) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
        this.status = status;
        this.statusField = statusField;
        this.statusLoc = namePositionMap.get(statusField);
        this.timestampLoc = namePositionMap.get(timestampField);
        this.copyIdFrom = copyIdFrom;
        this.copyIdTo = copyIdTo;
        this.idsKeys = idsKeys;
        this.entityKeys = entityKeys;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple result = Tuple.size(getFieldDeclaration().size());
        setupTupleForGroup(result, arguments);
        String status = arguments.getString(statusField);
        if (UPDATED.equals(this.status)) {
            if (arguments.getObject(copyIdFrom) != null) {
                result.set(statusLoc, this.status);
                result.set(timestampLoc, System.currentTimeMillis());
            }
        } else if (!this.status.equals(status)) {
            result.set(statusLoc, this.status);
            result.set(timestampLoc, System.currentTimeMillis());
        }
        if (idsKeys != null && entityKeys != null) {
            for (int i = 0; i < idsKeys.size(); i++) {
                result.set(namePositionMap.get(idsKeys.get(i)),
                        arguments.getString(entityKeys.get(i)));
            }
        }
        if (copyIdFrom != null && copyIdTo != null && arguments.getObject(copyIdFrom) != null
                && !(ACTIVE.equals(this.status) && ACTIVE.equals(status))) {
            for (String id : copyIdTo) {
                result.set(namePositionMap.get(id), arguments.getObject(copyIdFrom));
            }
        }
        functionCall.getOutputCollector().add(result);
    }

    private Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field : fieldDeclaration) {
            String fieldName = (String) field;
            positionMap.put(fieldName, pos++);
        }
        return positionMap;
    }

    private void setupTupleForGroup(Tuple result, TupleEntry arguments) {
        for (Object field : getFieldDeclaration()) {
            String fieldName = (String) field;
            Integer loc = namePositionMap.get(fieldName);
            if (loc != null && loc >= 0) {
                result.set(loc, arguments.getObject(fieldName));
            }
        }
    }

}
