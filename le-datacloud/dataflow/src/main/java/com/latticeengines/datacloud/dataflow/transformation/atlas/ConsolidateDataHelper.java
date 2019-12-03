package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;

public class ConsolidateDataHelper {

    private static final Logger log = LoggerFactory.getLogger(ConsolidateDataHelper.class);

    static void preProcessSources(List<String> sourceNames, List<Node> sources,
            Map<String, Map<String, String>> dupeFieldMap,
            List<String> fieldToRetain, Set<String> commonFields) {
        List<String> dupeFields = new ArrayList<>();
        for (int i = 0; i < sourceNames.size(); i++) {
            Node source = sources.get(i);
            Map<String, String> dupeFieldMapPerTable = checkAndSetDupeMap(sourceNames.get(i), dupeFieldMap);
            List<String> fieldNames = source.getFieldNames();
            List<String> oldNames = new ArrayList<>();
            List<String> newNames = new ArrayList<>();
            for (String fieldName : fieldNames) {
                if (!dupeFields.contains(fieldName)) {
                    dupeFields.add(fieldName);
                    fieldToRetain.add(fieldName);
                } else {
                    commonFields.add(fieldName);
                    String newFieldName = fieldName + "__DUPE__" + i;
                    oldNames.add(fieldName);
                    newNames.add(newFieldName);
                    dupeFieldMapPerTable.put(fieldName, newFieldName);

                }
            }
            if (newNames.size() > 0) {
                source = source.rename(new FieldList(oldNames), new FieldList(newNames));
                source = source.retain(new FieldList(source.getFieldNames()));
                sources.set(i, source);
            }
        }
    }

    private static Map<String, String> checkAndSetDupeMap(String sourceName,
            Map<String, Map<String, String>> dupeFieldMap) {
        dupeFieldMap.putIfAbsent(sourceName, new HashMap<>());
        return dupeFieldMap.get(sourceName);
    }

    static List<FieldList> getGroupFieldList(List<String> sourceNames, List<Table> sourceTables,
            Map<String, Map<String, String>> dupeFieldMap, String srcIdField) {
        List<FieldList> fieldLists = new ArrayList<>();
        if (StringUtils.isNotEmpty(srcIdField)) {
            fieldLists.add(new FieldList(srcIdField));
            for (int i = 1; i < sourceNames.size(); i++)
                fieldLists.add(new FieldList(srcIdField + "__DUPE__" + i));
            return fieldLists;
        }
        for (int i = 0; i < sourceNames.size(); i++) {
            Table table = sourceTables.get(i);
            Map<String, String> dupeFieldMapPerTable = dupeFieldMap.get(sourceNames.get(i));
            List<String> keyAttributes = new ArrayList<>();
            List<String> attributes = table.getPrimaryKey().getAttributes();
            for (String attribute : attributes) {
                keyAttributes.add(dupeFieldMapPerTable.getOrDefault(attribute, attribute));
            }
            fieldLists.add(new FieldList(keyAttributes));
        }
        return fieldLists;
    }

    static Node addTimestampColumns(Node input) {
        Date now = new Date();
        Node output = dropReservedColumns(input);
        output = output.addTimestamp(InterfaceName.CDLCreatedTime.name(), now);
        output = output.addTimestamp(InterfaceName.CDLUpdatedTime.name(), now);
        return output;
    }

    private static Node dropReservedColumns(Node input) {
        List<String> inputFields = input.getFieldNames().stream().map(String::toLowerCase).collect(Collectors.toList());
        List<String> fieldsToDrop = new ArrayList<>();
        for (String reserved: Arrays.asList(InterfaceName.CDLCreatedTime.name(), InterfaceName.CDLUpdatedTime.name())) {
            if (inputFields.contains(reserved.toLowerCase())) {
                fieldsToDrop.add(reserved);
            }
        }
        if (!fieldsToDrop.isEmpty()) {
            log.info("Dropping reserved columns: " + StringUtils.join(fieldsToDrop, ", "));
            return input.discard(new FieldList(fieldsToDrop));
        } else {
            return input;
        }
    }

}
