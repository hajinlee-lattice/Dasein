package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.metadata.Table;

public class ConsolidateDataHelper {

    void preProcessSources(List<String> sourceNames, List<Node> sources, Map<String, Map<String, String>> dupeFieldMap,
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

    private Map<String, String> checkAndSetDupeMap(String sourceName, Map<String, Map<String, String>> dupeFieldMap) {
        dupeFieldMap.putIfAbsent(sourceName, new HashMap<String, String>());
        Map<String, String> dupeFieldMapPerTable = dupeFieldMap.get(sourceName);
        return dupeFieldMapPerTable;
    }

    List<FieldList> getGroupFieldList(List<String> sourceNames, List<Table> sourceTables,
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
                if (dupeFieldMapPerTable.containsKey(attribute)) {
                    keyAttributes.add(dupeFieldMapPerTable.get(attribute));
                } else {
                    keyAttributes.add(attribute);
                }
            }
            fieldLists.add(new FieldList(keyAttributes));
        }
        return fieldLists;
    }

}
