package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.Range;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class BomboraSurgeIntentFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 7488201881918114688L;

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(BomboraSurgeIntentFunction.class);

    private Map<String, Integer> namePositionMap;
    private Map<String, Map<Range<Integer>, String>> intentMap;
    private String compoScoreField;
    private String bucketCodeField;
    private int intentLoc;

    public BomboraSurgeIntentFunction(String intentField, String compoScoreField, String bucketCodeField,
            Map<String, Map<Range<Integer>, String>> intentMap) {
        super(new Fields(intentField));
        this.namePositionMap = getPositionMap(fieldDeclaration);
        this.intentMap = intentMap;
        this.compoScoreField = compoScoreField;
        this.bucketCodeField = bucketCodeField;
        this.intentLoc = this.namePositionMap.get(intentField);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Integer compoScore = (Integer) arguments.getObject(compoScoreField);
        String bucketCode = arguments.getString(bucketCodeField);
        Map<Range<Integer>, String> compoRangeIntent = intentMap.get(bucketCode);
        Tuple result = Tuple.size(getFieldDeclaration().size());
        for (Range<Integer> range : compoRangeIntent.keySet()) {
            if (range.contains(compoScore)) {
                result.set(intentLoc, compoRangeIntent.get(range));
                break;
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
}
