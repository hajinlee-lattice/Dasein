package com.latticeengines.datacloud.dataflow.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;


public class BitEncodeUtils {

    private static ObjectMapper objectMapper = new ObjectMapper();

    // return group-by fields and target encoded fields
    public static Node encode(Node node, String[] groupByFields, List<SourceColumn> columns) {
        Map<String, EnrichedBitCodeBook> codeBookMap = getCodeBooks(columns);
        List<Node> encodedNodes = new ArrayList<>();

        List<String> retainedFields = new ArrayList<>(Arrays.asList(groupByFields));

        for (Map.Entry<String, EnrichedBitCodeBook> codeBookEntry : codeBookMap.entrySet()) {
            String targetColumn = codeBookEntry.getKey();
            String keyColumn = codeBookEntry.getValue().getKeyColumn();
            String valueColumn = codeBookEntry.getValue().getValueColumn();
            BitCodeBook codeBook = codeBookEntry.getValue().getBitCodeBook();
            Node encodedNode = node.bitEncode(groupByFields, keyColumn, valueColumn, targetColumn, codeBook);
            encodedNode = encodedNode.renamePipe("encoded-" + targetColumn);
            encodedNodes.add(encodedNode);
            retainedFields.add(targetColumn);
        }

        Node join = null;
        for (Node encodedNode : encodedNodes) {
            if (join == null) {
                join = encodedNode.renamePipe("join-encoded");
            } else {
                join = join.outerJoin(new FieldList(groupByFields), encodedNode, new FieldList(groupByFields));
            }
        }

        return join.retain(new FieldList(retainedFields.toArray(new String[retainedFields.size()])));
    }

    private static Map<String, EnrichedBitCodeBook> getCodeBooks(List<SourceColumn> columns) {

        Map<String, List<SourceColumn>> columnGroups = new HashMap<>();
        Map<String, EnrichedBitCodeBook> codeBookMap = new HashMap<>();

        for (SourceColumn column : columns) {
            SourceColumn.Calculation calculation = column.getCalculation();
            if (calculation != null && calculation.equals(SourceColumn.Calculation.BIT_ENCODE)) {
                String arguments = column.getArguments();
                if (StringUtils.isEmpty(arguments)) {
                    throw new IllegalArgumentException(
                            "Arguments of a column with calculation type BIT_ENCODE cannot be null: "
                                    + column.getColumnName());
                }

                JsonNode jsonNode;
                try {
                    jsonNode = objectMapper.readTree(arguments);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to parse arguments to json for column " + column.getColumnName(),
                            e);
                }

                String targetColumn = jsonNode.get("TargetColumn").asText();
                if (!columnGroups.containsKey(targetColumn)) {
                    columnGroups.put(targetColumn, new ArrayList<SourceColumn>());
                }
                columnGroups.get(targetColumn).add(column);
            }
        }

        for (Map.Entry<String, List<SourceColumn>> entry : columnGroups.entrySet()) {
            EnrichedBitCodeBook codeBook = getCodeBookForOneGroup(entry.getValue());
            codeBookMap.put(entry.getKey(), codeBook);
        }

        return codeBookMap;
    }

    private static EnrichedBitCodeBook getCodeBookForOneGroup(List<SourceColumn> columns) {
        String keyColumn = null;
        String valueColumn = null;
        BitCodeBook.Algorithm algorithm = null;
        String decodeStrategyStr = null;
        String valueDictStr = null;
        String bitUnitStr = null;
        Map<String, Integer> bitPosMap = new HashMap<>();

        for (SourceColumn column : columns) {
            String arguments = column.getArguments();

            JsonNode jsonNode;
            try {
                jsonNode = objectMapper.readTree(arguments);
            } catch (IOException e) {
                throw new RuntimeException("Failed to parse arguments to json for column " + column.getColumnName(), e);
            }

            String keyColumn1 = jsonNode.get("KeyColumn").asText();
            if (StringUtils.isEmpty(keyColumn)) {
                keyColumn = keyColumn1;
            } else if (!keyColumn.equals(keyColumn1)) {
                throw new IllegalArgumentException(
                        "KeyColumn specified in SourceColumn table mismatch within one group: " + keyColumn1 + " vs "
                                + keyColumn);
            }

            String valueColumn1 = jsonNode.has("ValueColumn") ? jsonNode.get("ValueColumn").asText() : null;
            if (StringUtils.isEmpty(valueColumn)) {
                valueColumn = valueColumn1;
            } else if (!valueColumn.equals(valueColumn1)) {
                throw new IllegalArgumentException(
                        "ValueColumn specified in SourceColumn table mismatch within one group: " + valueColumn1
                                + " vs " + valueColumn);
            }

            String algorithmStr = jsonNode.get("Algorithm").asText();
            BitCodeBook.Algorithm algorithm1 = BitCodeBook.Algorithm.valueOf(algorithmStr);
            if (algorithm == null) {
                algorithm = algorithm1;
            } else if (!algorithm.equals(algorithm1)) {
                throw new IllegalArgumentException(
                        "Algorithm specified in SourceColumn table mismatch within one group: " + algorithm1 + " vs "
                                + algorithm);
            }

            Integer bitPos = jsonNode.get("BitPosition").asInt();
            String[] targetKeys = jsonNode.get("TargetKeys").asText().split("\\|");

            String decodeStrategyStr1 = jsonNode.has("DecodeStrategy") ? jsonNode.get("DecodeStrategy").asText() : null;
            if (StringUtils.isEmpty(decodeStrategyStr)) {
                decodeStrategyStr = decodeStrategyStr1;
            } else if (!decodeStrategyStr.equals(decodeStrategyStr1)) {
                throw new IllegalArgumentException(
                        "DecodeStrategy specified in SourceColumn table mismatch within one group: "
                                + decodeStrategyStr1 + " vs " + decodeStrategyStr);
            }

            String valueDictStr1 = jsonNode.has("ValueDict") ? jsonNode.get("ValueDict").asText() : null;
            if (StringUtils.isEmpty(valueDictStr)) {
                valueDictStr = valueDictStr1;
            } else if (!valueDictStr.equals(valueDictStr1)) {
                throw new IllegalArgumentException(
                        "ValueDict specified in SourceColumn table mismatch within one group: " + valueDictStr1 + " vs "
                                + valueDictStr);
            }

            String bitUnitStr1 = jsonNode.has("BitUnit") ? jsonNode.get("BitUnit").asText() : null;
            if (StringUtils.isEmpty(bitUnitStr)) {
                bitUnitStr = bitUnitStr1;
            } else if (!bitUnitStr.equals(bitUnitStr1)) {
                throw new IllegalArgumentException("BitUnit specified in SourceColumn table mismatch within one group: "
                        + bitUnitStr1 + " vs " + bitUnitStr);
            }

            switch (algorithm) {
            case KEY_EXISTS:
                for (String key : targetKeys) {
                    bitPosMap.put(key, bitPos);
                }
                break;
            default:
            }
        }

        BitCodeBook.DecodeStrategy decodeStrategy = null;
        if (StringUtils.isNotEmpty(decodeStrategyStr)) {
            decodeStrategy = BitCodeBook.DecodeStrategy.valueOf(decodeStrategyStr);
        }

        Map<Object, String> valueDict = new HashMap<>();
        if (StringUtils.isNotEmpty(valueDictStr)) {
            try {
                String[] valueDictArr = valueDictStr.split("\\|\\|");
                switch (decodeStrategy) {
                case ENUM_STRING:
                    for (int i = 0; i < valueDictArr.length; i++) {
                        valueDict.put(valueDictArr[i], Integer.toBinaryString(i + 1));  // All 0s is for null
                    }
                    break;
                default:
                    break;
                }
            } catch (Exception ex) {
                throw new RuntimeException(String.format(
                        "Fail to parse ValueDict %s. ENUM_STRING strategy: concatenate all possible values by ||",
                        valueDictStr));
            }
        }

        Integer bitUnit = null;
        if (StringUtils.isNotEmpty(bitUnitStr)) {
            try {
                bitUnit = Integer.valueOf(bitUnitStr);
            } catch (NumberFormatException e) {
                throw new RuntimeException("Fail to parse BitUnit " + bitUnitStr, e);
            }
        }

        // If encode attr is in ValueColumn, DecodeStrategy and BitUnit are required
        if (StringUtils.isNotEmpty(valueColumn)) {
            if (decodeStrategy == null || bitUnit == null) {
                throw new RuntimeException(
                        "DecodeStrategy, ValueDict or BitUnit is empty for ValueColumn " + valueColumn);
            }
        }

        EnrichedBitCodeBook codeBook = new EnrichedBitCodeBook();
        codeBook.setKeyColumn(keyColumn);
        codeBook.setValueColumn(valueColumn);

        BitCodeBook internalCodeBook = new BitCodeBook(algorithm, decodeStrategy);
        internalCodeBook.setBitsPosMap(bitPosMap);
        internalCodeBook.setValueDict(valueDict);
        internalCodeBook.setBitUnit(bitUnit);
        codeBook.setBitCodeBook(internalCodeBook);

        return codeBook;
    }

    private static class EnrichedBitCodeBook {
        private BitCodeBook bitCodeBook;
        private String keyColumn;
        private String valueColumn;

        public BitCodeBook getBitCodeBook() {
            return bitCodeBook;
        }

        public void setBitCodeBook(BitCodeBook bitCodeBook) {
            this.bitCodeBook = bitCodeBook;
        }

        public String getKeyColumn() {
            return keyColumn;
        }

        public void setKeyColumn(String keyColumn) {
            this.keyColumn = keyColumn;
        }

        public String getValueColumn() {
            return valueColumn;
        }

        public void setValueColumn(String valueColumn) {
            this.valueColumn = valueColumn;
        }
    }

}
