package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.KVDepivotStrategy;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.DiscreteBucket;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class DiscreteProfileBuffer extends BaseOperation implements Buffer {
    private static final long serialVersionUID = -8693085617747722200L;

    private int maxDisVals;
    private Map<String, String> decStrs;

    private Map<String, Integer> namePositionMap;

    public DiscreteProfileBuffer(Fields fieldDeclaration, int maxDisVals,
            Map<String, String> decStrs) {
        super(fieldDeclaration);
        this.maxDisVals = maxDisVals;
        this.decStrs = decStrs;
        this.namePositionMap = getPositionMap(fieldDeclaration);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Set<Integer> intDict = new HashSet<>();
        Set<Long> longDict = new HashSet<>();
        Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
        boolean isInt = true;
        while (iter.hasNext()) {
            TupleEntry arguments = iter.next();
            isInt = processData(arguments, intDict, longDict);
        }
        if (isInt) {
            outputIntResult(bufferCall, intDict);
        } else {
            outputLongResult(bufferCall, longDict);
        }
    }

    private boolean processData(TupleEntry arguments, Set<Integer> intDict, Set<Long> longDict) {
        String attr = arguments.getString(KVDepivotStrategy.KEY_ATTR);
        boolean isInt = arguments.getObject(KVDepivotStrategy.valueAttr(Integer.class)) != null;
        if (isInt) {
            insertIntDict(attr, arguments.getObject(KVDepivotStrategy.valueAttr(Integer.class)),
                    intDict);
        } else {
            insertLongDict(attr, arguments.getObject(KVDepivotStrategy.valueAttr(Long.class)),
                    longDict);
        }
        return isInt;
    }

    private void insertIntDict(String attr, Object val, Set<Integer> intDict) {
        if (intDict.contains(null) || val == null) {
            return;
        }
        intDict.add((Integer) val);
        if (intDict.size() > maxDisVals) {
            intDict.clear();
            intDict.add(null); // this attribute is not discrete attribute
        }
    }

    private void insertLongDict(String attr, Object val, Set<Long> longDict) {
        if (longDict.contains(null) || val == null) {
            return;
        }
        longDict.add((Long) val);
        if (longDict.size() > maxDisVals) {
            longDict.clear();
            longDict.add(null); // this attribute is not discrete attribute
        }
    }

    private void outputIntResult(BufferCall bufferCall, Set<Integer> intDict) {
        if (intDict.contains(null) || intDict.isEmpty()) {
            return;
        }
        List<Number> values = new ArrayList<>();
        values.addAll(intDict);
        outputResult(bufferCall, values);
    }

    private void outputLongResult(BufferCall bufferCall, Set<Long> longDict) {
        if (longDict.contains(null) || longDict.isEmpty()) {
            return;
        }
        List<Number> values = new ArrayList<>();
        values.addAll(longDict);
        outputResult(bufferCall, values);
    }

    private void outputResult(BufferCall bufferCall, List<Number> values) {
        String attr = bufferCall.getGroup().getString(KVDepivotStrategy.KEY_ATTR);
        Tuple result = Tuple.size(getFieldDeclaration().size());
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_ATTRNAME), attr);
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_SRCATTR), attr);
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_DECSTRAT),
                decStrs.get(attr));
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_ENCATTR), null);
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_LOWESTBIT), null);
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_NUMBITS), null);
        DiscreteBucket bucket = new DiscreteBucket();
        bucket.setValues(values);
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_BKTALGO),
                JsonUtils.serialize(bucket));
        bufferCall.getOutputCollector().add(result);
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
}
