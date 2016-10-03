package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
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
    private String[] decodeFields;

    public BitDecodeFunction(Fields fieldDeclaration, String encodedColumn, String[] decodedFields,
            BitCodeBook codeBook) {
        super(1, fieldDeclaration);
        if (codeBook.getDecodeStrategy() == null) {
            throw new IllegalArgumentException("Cannot find decode stragety in the code book.");
        }
        this.codeBook = codeBook;
        this.encodedColumn = encodedColumn;
        this.decodeFields = decodedFields;
        assignBitPos();
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

    private void assignBitPos() {
        switch (codeBook.getDecodeStrategy()) {
        case BOOLEAN_YESNO:
            assignSingleDigitBitPos();
            break;
        default:
        }
    }

    private void assignSingleDigitBitPos() {
        List<Integer> bitPosList = new ArrayList<>();
        for (String decodeField : decodeFields) {
            if (codeBook.hasKey(decodeField)) {
                int bitPos = codeBook.getBitPosForkey(decodeField);
                bitPositionIdx.put(decodeField, bitPosList.size());
                bitPosList.add(bitPos);
            } else {
                log.warn("Cannot find field " + decodeField + " from the codebook.");
            }
        }
        bitPositions = ArrayUtils.toPrimitive(bitPosList.toArray(new Integer[bitPosList.size()]));
    }

    private Tuple decode(TupleEntry arguments) {
        String encodedStr = arguments.getString(encodedColumn);

        boolean[] bits;
        try {
            bits = BitCodecUtils.decode(encodedStr, bitPositions);
        } catch (IOException e) {
            throw new RuntimeException("Failed to decode string " + encodedStr, e);
        }

        Map<String, Object> valueMap = translateBits(bits);
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

    private Map<String, Object> translateBits(boolean[] bits) {
        switch (codeBook.getDecodeStrategy()) {
        case BOOLEAN_YESNO:
            return translateBitsToYesNo(bits);
        default:
            throw new UnsupportedOperationException("Unsupported decode strategy " + codeBook.getDecodeStrategy());
        }
    }

    private Map<String, Object> translateBitsToYesNo(boolean[] bits) {
        Map<String, Object> valueMap = new HashMap<>();
        for (String decodeField : decodeFields) {
            if (bitPositionIdx.containsKey(decodeField)) {
                int idx = bitPositionIdx.get(decodeField);
                boolean bit = bits[idx];
                valueMap.put(decodeField, bit ? "Yes" : "No");
            }
        }
        return valueMap;
    }

}
