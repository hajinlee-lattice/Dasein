package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.CategoricalBucket;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class CategoricalProfileBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = -5754107534460911952L;

    private String catAttrField;
    private String catValueField;
    private String nonCatFlag;
    private int maxCat;

    private Map<String, Integer> namePositionMap;

    public CategoricalProfileBuffer(Fields fieldDeclaration, String catAttrField, String catValueField,
            String nonCatFlag, int maxCat) {
        super(fieldDeclaration);
        this.catAttrField = catAttrField;
        this.catValueField = catValueField;
        this.nonCatFlag = nonCatFlag;
        this.maxCat = maxCat;
        this.namePositionMap = getPositionMap(fieldDeclaration);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Set<String> dict = new HashSet<>();
        Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
        while (iter.hasNext()) {
            TupleEntry arguments = iter.next();
            processData(arguments, dict);
        }
        outputResult(bufferCall, dict);
    }

    private void processData(TupleEntry arguments, Set<String> dict) {
        String val = arguments.getString(catValueField);
        if (val == null || dict.contains(nonCatFlag)) {
            return;
        }
        if (nonCatFlag.equals(val)) {
            dict.clear();
            dict.add(nonCatFlag);
            return;
        }
        dict.add(val);
        if (dict.size() > maxCat) {
            dict.clear();
            dict.add(nonCatFlag);
        }
    }

    private void outputResult(BufferCall bufferCall, Set<String> dict) {
        String attr = bufferCall.getGroup().getString(catAttrField);
        Tuple result = Tuple.size(getFieldDeclaration().size());
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_ATTRNAME), attr);
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_SRCATTR), attr);
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_DECSTRAT), null);
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_ENCATTR), null);
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_LOWESTBIT), null);
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_NUMBITS), null);
        if (dict.size() == 0 || dict.contains(nonCatFlag)) {
            result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_BKTALGO), null);
        } else {
            CategoricalBucket bucket = new CategoricalBucket();
            bucket.setCategories(new ArrayList<>(dict));
            result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_BKTALGO), JsonUtils.serialize(bucket));
        }
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
