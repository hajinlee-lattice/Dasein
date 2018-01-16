package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

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
public class BitDecodeAndEncodeFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = -1829655353767648350L;

    private final BitCodeBook decodeBook;
    private final String encodedColumn;
    private final String newEncodedColumn;
    private int[] decodeBitPositions;
    private Map<String, Integer> decodeBitPosIdx = new HashMap<>();
    private int[] encodeBitPositions;
    private Map<String, Integer> encodeBitPosIdx = new HashMap<>();
    private Map<Integer, Integer> decBitPosToEncBitPos = new HashMap<>();
    private List<String> decodeFields;

    public BitDecodeAndEncodeFunction(String newEncodedColumn, String encodedColumn, List<String> decodedFields,
            BitCodeBook decodeBook, BitCodeBook encodeBook) {
        super(new Fields(newEncodedColumn));
        if (decodeBook.getDecodeStrategy() == null) {
            throw new IllegalArgumentException("Cannot find decode strategy in the decode book.");
        }
        encodeBook.setDecodeStrategy(decodeBook.getDecodeStrategy());
        this.decodeBook = decodeBook;
        this.encodedColumn = encodedColumn;
        this.newEncodedColumn = newEncodedColumn;
        this.decodeFields = decodedFields;

        decodeBitPositions = decodeBook.assignBitPosAndUpdateIdxMap(this.decodeFields, decodeBitPosIdx);
        encodeBitPositions = encodeBook.assignBitPosAndUpdateIdxMap(this.decodeFields, encodeBitPosIdx);

        int bitUnit = findBitUnit();
        for (String decodeField : decodeFields) {
            for (int i = 0; i < bitUnit; i++) {
                decBitPosToEncBitPos.put(decodeBitPositions[decodeBitPosIdx.get(decodeField) + i],
                        encodeBitPositions[encodeBitPosIdx.get(decodeField) + i]);
            }
        }
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple result = decodeAndEncode(arguments);
        if (result != null) {
            functionCall.getOutputCollector().add(result);
        } else {
            functionCall.getOutputCollector().add(Tuple.size(1));
        }
    }

    private Tuple decodeAndEncode(TupleEntry arguments) {
        String encodedStr = arguments.getString(encodedColumn);
        if (StringUtils.isEmpty(encodedStr)) {
            return null;
        }

        boolean[] decodedBits;
        try {
            decodedBits = BitCodecUtils.decode(encodedStr, decodeBitPositions);
        } catch (IOException e) {
            throw new RuntimeException("Failed to decode string " + encodedStr, e);
        }

        List<Integer> trueBitsToEnc = new ArrayList<>();
        for (int i = 0; i < decodedBits.length; i++) {
            if (decodedBits[i]) {
                trueBitsToEnc.add(decBitPosToEncBitPos.get(decodeBitPositions[i]));
            }
        }

        try {
            int[] trueBitsArray = ArrayUtils.toPrimitive(trueBitsToEnc.toArray(new Integer[trueBitsToEnc.size()]));
            String value = BitCodecUtils.encode(trueBitsArray);
            return new Tuple(value);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Fail to encode %s to %s", encodedColumn, newEncodedColumn), e);
        }
    }

    private int findBitUnit() {
        Integer bitUnit = null;
        switch (decodeBook.getDecodeStrategy()) {
        case BOOLEAN_YESNO:
            bitUnit = 1;
            break;
        default:
            bitUnit = decodeBook.getBitUnit();
            break;
        }
        if (bitUnit == null) {
            throw new RuntimeException("Fail to find bit unit for decode strategy " + decodeBook.getDecodeStrategy());
        }
        return bitUnit;
    }

}
