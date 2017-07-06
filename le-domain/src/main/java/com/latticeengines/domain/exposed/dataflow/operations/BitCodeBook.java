package com.latticeengines.domain.exposed.dataflow.operations;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.common.exposed.util.BitCodecUtils;

public class BitCodeBook implements Serializable {

    private static final long serialVersionUID = -7220566464433207501L;
    private Map<String, Integer> bitsPosMap;
    private Algorithm encodeAlgo;
    private DecodeStrategy decodeStrategy;
    private String encodedColumn;
    private Map<Object, String> valueDict; // eg. A||B||C -> null:00, A:01, B:10, C:11
    private Map<String, Object> valueDictRev;   // eg. A||B||C -> 00:null, 01:A, 10:B, 11:C
    private Integer bitUnit;

    public BitCodeBook() {
        super();
    }

    public BitCodeBook(Algorithm encodeAlgo) {
        this.encodeAlgo = encodeAlgo;
        this.decodeStrategy = null;
    }

    public BitCodeBook(DecodeStrategy decodeStrategy) {
        this.decodeStrategy = decodeStrategy;
        this.encodeAlgo = null;
    }

    public BitCodeBook(Algorithm encodeAlgo, DecodeStrategy decodeStrategy) {
        this.decodeStrategy = decodeStrategy;
        this.encodeAlgo = encodeAlgo;
    }

    public void setBitsPosMap(Map<String, Integer> bitsPosMap) {
        Map<String, Integer> copy = new HashMap<>();
        if (bitsPosMap != null) {
            for (Map.Entry<String, Integer> entry : bitsPosMap.entrySet()) {
                copy.put(entry.getKey(), entry.getValue());
            }
        }
        this.bitsPosMap = Collections.unmodifiableMap(copy);
    }

    public Algorithm getEncodeAlgo() {
        return encodeAlgo;
    }

    public DecodeStrategy getDecodeStrategy() {
        return decodeStrategy;
    }

    public void setEncodeAlgo(Algorithm encodeAlgo) {
        this.encodeAlgo = encodeAlgo;
    }

    public void setDecodeStrategy(DecodeStrategy decodeStrategy) {
        this.decodeStrategy = decodeStrategy;
    }

    public Integer getBitPosForKey(String key) {
        return bitsPosMap.get(key);
    }

    public String getEncodedColumn() {
        return encodedColumn;
    }

    public void bindEncodedColumn(String encodedColumn) {
        this.encodedColumn = encodedColumn;
    }

    public boolean hasKey(String key) {
        return bitsPosMap.keySet().contains(key);
    }

    public Map<String, Integer> getBitsPosMap() {
        return bitsPosMap;
    }

    public Map<Object, String> getValueDict() {
        return valueDict;
    }

    public void setValueDict(Map<Object, String> valueDict) {
        this.valueDict = valueDict;
    }

    public Integer getBitUnit() {
        return bitUnit;
    }

    public void setBitUnit(Integer bitUnit) {
        this.bitUnit = bitUnit;
    }

    public Map<String, Object> getValueDictRev() {
        return valueDictRev;
    }

    public void setValueDictRev(Map<String, Object> valueDictRev) {
        this.valueDictRev = valueDictRev;
    }

    public enum Algorithm {
        KEY_EXISTS
    }

    public enum DecodeStrategy {
        BOOLEAN_YESNO, //
        NUMERIC_INT, //
        NUMERIC_UNSIGNED_INT, //
        ENUM_STRING, //
    }

    public Map<String, Object> decode(String encodedStr, List<String> decodeFields) {
        if (getDecodeStrategy() == null) {
            throw new IllegalArgumentException("Must provide decode strategy to decode.");
        }

        if (StringUtils.isEmpty(encodedStr)) {
            return Collections.emptyMap();
        }

        Map<String, Integer> bitPositionIdx = new HashMap<>();
        int[] bitPositions = assignBitPosAndUpdateIdxMap(decodeFields, bitPositionIdx);

        try {
            boolean[] bits = BitCodecUtils.decode(encodedStr, bitPositions);
            return translateBits(bits, decodeFields, bitPositionIdx);
        } catch (IOException e) {
            throw new RuntimeException("Failed to decode " + encodedStr + " for fields "
                    + ArrayUtils.toString(decodeFields), e);
        }
    }

    public int[] assignBitPosAndUpdateIdxMap(List<String> decodeFields, Map<String, Integer> bitPositionIdx) {
        switch (getDecodeStrategy()) {
        case BOOLEAN_YESNO:
            return assignSingleDigitBitPos(decodeFields, bitPositionIdx);
        case NUMERIC_INT:
        case NUMERIC_UNSIGNED_INT:
        case ENUM_STRING:
            return assignMultipleDigitBitPos(decodeFields, bitPositionIdx);
        default:
            return null;
        }
    }

    private int[] assignSingleDigitBitPos(List<String> decodeFields, Map<String, Integer> bitPositionIdx) {
        List<Integer> bitPoses = new ArrayList<>();
        for (String field : decodeFields) {
            Integer bitPos = getBitPosForKey(field);
            if (bitPos != null) {
                bitPositionIdx.put(field, bitPoses.size());
                bitPoses.add(bitPos);
            }
        }
        return ArrayUtils.toPrimitive(bitPoses.toArray(new Integer[bitPoses.size()]));
    }

    private int[] assignMultipleDigitBitPos(List<String> decodeFields, Map<String, Integer> bitPositionIdx) {
        List<Integer> bitPoses = new ArrayList<>();
        for (String field : decodeFields) {
            Integer bitPos = getBitPosForKey(field);
            if (bitPos != null) {
                bitPositionIdx.put(field, bitPoses.size());
                for (int i = 0; i < getBitUnit(); i++) {
                    bitPoses.add(bitPos + i);
                }
            }
        }
        return ArrayUtils.toPrimitive(bitPoses.toArray(new Integer[bitPoses.size()]));
    }

    public Map<String, Object> translateBits(boolean[] bits, List<String> decodeFields,
            Map<String, Integer> bitPositionIdx) {
        switch (getDecodeStrategy()) {
        case BOOLEAN_YESNO:
            return translateBitsToYesNo(bits, decodeFields, bitPositionIdx);
        case NUMERIC_INT:
            return translateBitsToInt(bits, decodeFields, bitPositionIdx, true);
        case NUMERIC_UNSIGNED_INT:
            return translateBitsToInt(bits, decodeFields, bitPositionIdx, false);
        case ENUM_STRING:
            return translateBitsToEnumString(bits, decodeFields, bitPositionIdx);
        default:
            throw new UnsupportedOperationException("Unsupported decode strategy " + getDecodeStrategy());
        }
    }

    private Map<String, Object> translateBitsToYesNo(boolean[] bits, List<String> decodeFields,
            Map<String, Integer> bitPositionIdx) {
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

    private Map<String, Object> translateBitsToInt(boolean[] bits, List<String> decodeFields,
            Map<String, Integer> bitPositionIdx, boolean signed) {
        Map<String, Object> valueMap = new HashMap<>();
        for (String decodeField : decodeFields) {
            if (bitPositionIdx.containsKey(decodeField)) {
                int idx = bitPositionIdx.get(decodeField);
                boolean notNull = bits[idx + getBitUnit() - 1]; // highest bit is null indicator
                if (notNull) {
                    int value = 0;
                    for (int i = getBitUnit() - 2; i >= 0; i--) {
                        value = (value << 1) + (bits[idx + i] ? 1 : 0);
                    }
                    valueMap.put(decodeField, value);
                } else {
                    valueMap.put(decodeField, null);
                }
            }
        }
        return valueMap;
    }

    private Map<String, Object> translateBitsToEnumString(boolean[] bits, List<String> decodeFields,
            Map<String, Integer> bitPositionIdx) {
        Map<String, Object> valueMap = new HashMap<>();
        for (String decodeField : decodeFields) {
            if (bitPositionIdx.containsKey(decodeField)) {
                int idx = bitPositionIdx.get(decodeField);
                int value = 0;
                for (int i = getBitUnit() - 1; i >= 0; i--) {
                    value = (value << 1) + (bits[idx + i] ? 1 : 0);
                }
                valueMap.put(decodeField, getValueDictRev().get(Integer.toBinaryString(value)));
            }
        }
        return valueMap;
    }

}
