package com.latticeengines.datacloud.core.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;

public class BitCodeBookUtils {

    private static Log log = LogFactory.getLog(BitCodeBookUtils.class);

    /**
     * @param codeBookMap (empty map)
     * @param codeBookLookup (empty map)
     * @param decodeStrs (decodedAttr -> decodeStrategy)
     */
    public static void constructCodeBookMap(Map<String, BitCodeBook> codeBookMap, Map<String, String> codeBookLookup,
            Map<String, String> decodeStrs) {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Map<String, Integer>> bitPosMap = new HashMap<>();
        Map<String, BitCodeBook.DecodeStrategy> decodeStrategyMap = new HashMap<>();
        Map<String, Map<String, Object>> valueDictRevMap = new HashMap<>();
        Map<String, Integer> bitUnitMap = new HashMap<>();
        for (Map.Entry<String, String> ent: decodeStrs.entrySet()) {
            String decodeStrategyStr = ent.getValue();
            if (StringUtils.isEmpty(decodeStrategyStr)) {
                continue;
            }
            JsonNode jsonNode;
            try {
                jsonNode = objectMapper.readTree(decodeStrategyStr);
            } catch (IOException e) {
                throw new RuntimeException("Failed to parse decodeStrategy " + decodeStrategyStr);
            }
            String encodedColumn = jsonNode.has("EncodedColumn") ? jsonNode.get("EncodedColumn").asText() : null;
            String columnName = ent.getKey();
            Integer bitPos = jsonNode.get("BitPosition").asInt();
            if (!bitPosMap.containsKey(encodedColumn)) {
                bitPosMap.put(encodedColumn, new HashMap<>());
            }
            bitPosMap.get(encodedColumn).put(columnName, bitPos);
            if (!decodeStrategyMap.containsKey(encodedColumn)) {
                String decodeStr = jsonNode.get("BitInterpretation").asText();
                try {
                    BitCodeBook.DecodeStrategy decodeStrategy = BitCodeBook.DecodeStrategy.valueOf(decodeStr);
                    decodeStrategyMap.put(encodedColumn, decodeStrategy);
                    switch (decodeStrategy) {
                    case ENUM_STRING:
                        String valueDictStr = jsonNode.get("ValueDict").asText();
                        String[] valueDictArr = valueDictStr.split("\\|\\|");
                        Map<String, Object> valueDictRev = new HashMap<>();
                        for (int i = 0; i < valueDictArr.length; i++) {
                            valueDictRev.put(Integer.toBinaryString(i + 1), valueDictArr[i]);
                        }
                        valueDictRevMap.put(encodedColumn, valueDictRev);
                    case NUMERIC_INT:
                    case NUMERIC_UNSIGNED_INT:
                        Integer bitUnit = jsonNode.get("BitUnit").asInt();
                        bitUnitMap.put(encodedColumn, bitUnit);
                        break;
                    default:
                        break;
                    }
                } catch (Exception e) {
                    log.error("Could not understand decode strategy", e);
                }
            }
            if (codeBookLookup.containsKey(columnName)) {
                throw new RuntimeException("Column " + columnName + " is already defined to use encoded column "
                        + codeBookLookup.get(columnName) + ", but now it is tried to use " + encodedColumn);
            }
            codeBookLookup.put(columnName, encodedColumn);
        }

        for (Map.Entry<String, Map<String, Integer>> entry : bitPosMap.entrySet()) {
            if (!decodeStrategyMap.containsKey(entry.getKey())) {
                throw new RuntimeException(
                        "Could not find a valid decode strategy for encoded column " + entry.getKey());
            }
        }

        for (Map.Entry<String, BitCodeBook.DecodeStrategy> entry : decodeStrategyMap.entrySet()) {
            if (!bitPosMap.containsKey(entry.getKey())) {
                throw new RuntimeException(
                        "Could not find a valid bit position map for encoded column " + entry.getKey());
            }
            BitCodeBook codeBook = new BitCodeBook(entry.getValue());
            codeBook.setBitsPosMap(bitPosMap.get(entry.getKey()));
            codeBook.setBitUnit(bitUnitMap.get(entry.getKey()));
            codeBook.setValueDictRev(valueDictRevMap.get(entry.getKey()));
            codeBookMap.put(entry.getKey(), codeBook);
        }
    }
}
