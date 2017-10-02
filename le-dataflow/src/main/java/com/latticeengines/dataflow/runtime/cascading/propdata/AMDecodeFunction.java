package com.latticeengines.dataflow.runtime.cascading.propdata;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AMDecodeFunction extends BaseOperation implements Function {
    private Map<String, BitCodeBook> codeBookMap;
    private Map<String, Integer> positionMap;
    private Map<String, List<String>> encode2Decode;

    public AMDecodeFunction(Fields fieldDeclaration, Map<String, String> codeBookLookup,
                            Map<String, BitCodeBook> codeBookMap) {
        super(fieldDeclaration);
        this.codeBookMap = codeBookMap;
        this.positionMap = getPositionMap(fieldDeclaration);
        this.encode2Decode = new HashMap<>();

        for (Object field : fieldDeclaration) {
            String fieldName = (String) field;
            String encodeName = codeBookLookup.get(fieldName);
            if (!encode2Decode.containsKey(encodeName)) {
                List<String> attributesToDecode = new ArrayList<>();
                attributesToDecode.add(fieldName);
                encode2Decode.put(encodeName, attributesToDecode);
            } else {
                encode2Decode.get(encodeName).add(fieldName);
            }
        }
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        Tuple tuple = Tuple.size(getFieldDeclaration().size());
        for (Map.Entry<String, BitCodeBook> entry : codeBookMap.entrySet()) {
            List<String> attributesToDecode = encode2Decode.get(entry.getKey());
            Map<String, Object> decodedValues = entry.getValue().decode(
                    functionCall.getArguments().getString(entry.getKey()), attributesToDecode);
            for (Map.Entry<String, Object> resultEntry : decodedValues.entrySet()) {
                tuple.set(positionMap.get(resultEntry.getKey()), resultEntry.getValue());
            }
        }
        functionCall.getOutputCollector().add(tuple);
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
