package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger log = LoggerFactory.getLogger(BomboraSurgeIntentFunction.class);

    private Map<String, Integer> namePositionMap;
    private Map<Range<Integer>, String> intentMap;
    private String compoScoreField;
    private int intentLoc;

    public BomboraSurgeIntentFunction(String intentField, String compoScoreField,
            Map<Range<Integer>, String> intentMap) {
        super(new Fields(intentField));
        this.namePositionMap = getPositionMap(fieldDeclaration);
        this.intentMap = intentMap;
        this.compoScoreField = compoScoreField;
        this.intentLoc = this.namePositionMap.get(intentField);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Integer compoScore = (Integer) arguments.getObject(compoScoreField);
        Tuple result = Tuple.size(getFieldDeclaration().size());
        for (Range<Integer> range : intentMap.keySet()) {
            if (range.contains(compoScore)) {
                result.set(intentLoc, intentMap.get(range));
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
