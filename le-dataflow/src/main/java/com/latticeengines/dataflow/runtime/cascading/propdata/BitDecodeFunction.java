package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.common.exposed.util.BitCodecUtils;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class BitDecodeFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = -1829655353767648350L;
    private static final Log log = LogFactory.getLog(BitDecodeFunction.class);

    private final BitCodeBook codeBook;
    private final String encodedColumn;
    private int[] bitPositions;
    private Map<String, Integer> bitPositionIdx = new HashMap<>();
    private List<String> decodeFields;

    public BitDecodeFunction(Fields fieldDeclaration, String encodedColumn, String[] decodedFields,
            BitCodeBook codeBook) {
        super(1, fieldDeclaration);
        if (codeBook.getDecodeStrategy() == null) {
            throw new IllegalArgumentException("Cannot find decode stragety in the code book.");
        }
        this.codeBook = codeBook;
        this.encodedColumn = encodedColumn;
        this.decodeFields = Arrays.asList(decodedFields);

        bitPositions = codeBook.assignBitPosAndUpdateIdxMap(this.decodeFields, bitPositionIdx);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple result = decode(arguments);
        if (result != null) {
            functionCall.getOutputCollector().add(result);
        } else {
            functionCall.getOutputCollector().add(Tuple.size(fieldDeclaration.size()));
        }
    }

    private Tuple decode(TupleEntry arguments) {
        String encodedStr = arguments.getString(encodedColumn);

        if (StringUtils.isEmpty(encodedStr)) {
            return null;
        }

        boolean[] bits;
        try {
            bits = BitCodecUtils.decode(encodedStr, bitPositions);
        } catch (IOException e) {
            throw new RuntimeException("Failed to decode string " + encodedStr, e);
        }

        Map<String, Object> valueMap = codeBook.translateBits(bits, decodeFields, bitPositionIdx);
        Tuple result = Tuple.size(fieldDeclaration.size());
        for (int i = 0; i < fieldDeclaration.size(); i++) {
            String fieldName = (String) fieldDeclaration.get(i);
            if (valueMap.containsKey(fieldName)) {
                result.set(i, valueMap.get(fieldName));
            } else {
                result.set(i, null);
            }
        }

        return result;
    }

}
