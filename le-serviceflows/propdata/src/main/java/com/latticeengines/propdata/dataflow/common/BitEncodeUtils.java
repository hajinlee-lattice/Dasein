package com.latticeengines.propdata.dataflow.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
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
                join = join.join(new FieldList(groupByFields), encodedNode, new FieldList(groupByFields),
                        JoinType.OUTER);
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
                        "ValueColumn specified in SourceColumn table mismatch within one group: " + algorithm1 + " vs "
                                + algorithm1);
            }

            Integer bitPos = jsonNode.get("BitPosition").asInt();
            String[] targetKeys = jsonNode.get("TargetKeys").asText().split("\\|");

            switch (algorithm) {
            case KEY_EXISTS:
                for (String key : targetKeys) {
                    bitPosMap.put(key, bitPos);
                }
                break;
            default:
            }
        }

        EnrichedBitCodeBook codeBook = new EnrichedBitCodeBook();
        codeBook.setKeyColumn(keyColumn);
        codeBook.setValueColumn(valueColumn);

        BitCodeBook internalCodeBook = new BitCodeBook(algorithm);
        internalCodeBook.setBitsPosMap(bitPosMap);
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
