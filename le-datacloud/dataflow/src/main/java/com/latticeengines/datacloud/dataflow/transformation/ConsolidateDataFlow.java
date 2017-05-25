package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.stereotype.Component;

import cascading.operation.Function;
import cascading.tuple.Fields;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.ConsolidateDataFuction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.Table;

@Component("consolidateDataFlow")
public class ConsolidateDataFlow extends ConfigurableFlowBase<ConsolidateDataTransformerConfig> {

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        ConsolidateDataTransformerConfig config = getTransformerConfig(parameters);

        List<Node> sources = new ArrayList<>();
        List<Table> sourceTables = new ArrayList<>();
        List<String> sourceNames = new ArrayList<>();
        for (int i = 0; i < parameters.getBaseTables().size(); i++) {
            String sourceName = parameters.getBaseTables().get(i);
            sources.add(addSource(sourceName));
            sourceTables.add(getSourceMetadata(sourceName));
            sourceNames.add(sourceName);
        }
        if (sources.size() <= 1) {
            return sources.get(0);
        }

        Map<String, Map<String, String>> dupeFieldMap = new LinkedHashMap<>();
        List<String> fieldToRetain = new ArrayList<>();
        Set<String> commonFields = new HashSet<>();
        preProcessSources(sourceNames, sources, dupeFieldMap, fieldToRetain, commonFields);

        List<FieldList> groupFieldLists = getGroupFieldList(sourceNames, sourceTables, dupeFieldMap);

        Node result = sources.get(0).coGroup(groupFieldLists.get(0), sources.subList(1, sources.size()),
                groupFieldLists.subList(1, groupFieldLists.size()), JoinType.OUTER);

        List<String> allFieldNames = result.getFieldNames();
        Function<?> function = new ConsolidateDataFuction(allFieldNames, commonFields, dupeFieldMap);
        result = result.apply(function, new FieldList(allFieldNames), getMetadata(result.getIdentifier()),
                new FieldList(fieldToRetain), Fields.REPLACE);
        result = result.retain(new FieldList(fieldToRetain));
        return result;
    }

    private void preProcessSources(List<String> sourceNames, List<Node> sources,
            Map<String, Map<String, String>> dupeFieldMap, List<String> fieldToRetain, Set<String> commonFields) {
        List<String> dupeFields = new ArrayList<>();
        for (int i = 0; i < sourceNames.size(); i++) {
            Node source = sources.get(i);
            Map<String, String> dupeFieldMapPerTable = checkAndSetDupeMap(sourceNames, dupeFieldMap, i);
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
                sources.set(i, source);
            }
        }
    }

    private Map<String, String> checkAndSetDupeMap(List<String> sourceNames,
            Map<String, Map<String, String>> dupeFieldMap, int i) {
        Map<String, String> dupeFieldMapPerTable = dupeFieldMap.get(sourceNames.get(i));
        if (dupeFieldMapPerTable == null) {
            dupeFieldMapPerTable = new HashMap<String, String>();
            dupeFieldMap.put(sourceNames.get(i), dupeFieldMapPerTable);
        }
        return dupeFieldMapPerTable;
    }

    private List<FieldList> getGroupFieldList(List<String> sourceNames, List<Table> sourceTables,
            Map<String, Map<String, String>> dupeFieldMap) {
        List<FieldList> fieldLists = new ArrayList<>();
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

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return ConsolidateDataTransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "consolidateDataFlow";
    }

    @Override
    public String getTransformerName() {
        return "consolidateDataTransformer";

    }
}