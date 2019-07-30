package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.cdl.TrimFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.ConsolidateDataFuction;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.Table;

import cascading.operation.Function;
import cascading.tuple.Fields;

/**
 * Used by mergeInputs and mergeMaster steps
 */
@Component(ConsolidateDataFlow.DATAFLOW_BEAN_NAME)
public class ConsolidateDataFlow extends ConsolidateBaseFlow<ConsolidateDataTransformerConfig> {

    public static final String DATAFLOW_BEAN_NAME = "consolidateDataFlow";
    public static final String TRANSFORMER_NAME = DataCloudConstants.TRANSFORMER_CONSOLIDATE_DATA;

    private static final String UUID = "__Id__";

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        ConsolidateDataTransformerConfig config = getTransformerConfig(parameters);
        Node result;

        List<Node> sources = new ArrayList<>();
        List<Table> sourceTables = new ArrayList<>();
        List<String> sourceNames = new ArrayList<>();

        String groupByKey = processIdColumns(parameters, config, sources, sourceTables, sourceNames);

        cloneSrcFlds(sources, config.getCloneSrcFields());
        renameSrcFlds(sources, config.getRenameSrcFields());

        if (config.isCreateTimestampColumn()) {
            createTimestampColumns(config, sources);
        }
        if (CollectionUtils.isNotEmpty(config.getCompositeKeys())) {
            groupByKey = buildNewIdColumn(config, sources);
        }

        dedupeSource(config, sources, groupByKey);
        if (config.isMergeOnly()) {
            addIdColumn(sources, UUID);
            groupByKey = UUID;
        }
        if (sources.size() <= 1) {
            result = sources.get(0);
        } else {
            Map<String, Map<String, String>> dupeFieldMap = new LinkedHashMap<>();
            List<String> fieldToRetain = new ArrayList<>();
            Set<String> commonFields = new HashSet<>();
            ConsolidateDataHelper.preProcessSources(sourceNames, sources, dupeFieldMap, fieldToRetain, commonFields);

            List<FieldList> groupFieldLists = ConsolidateDataHelper.getGroupFieldList(sourceNames, sourceTables,
                    dupeFieldMap, groupByKey);

            result = sources.get(0).coGroup(groupFieldLists.get(0), sources.subList(1, sources.size()),
                    groupFieldLists.subList(1, groupFieldLists.size()), JoinType.OUTER);

            List<String> allFieldNames = result.getFieldNames();
            Function<?> function = new ConsolidateDataFuction(allFieldNames, commonFields, dupeFieldMap,
                    config.getColumnsFromRight());
            result = result.apply(function, new FieldList(allFieldNames), result.getSchema(),
                    new FieldList(allFieldNames), Fields.REPLACE);

            result = result.retain(new FieldList(fieldToRetain));
        }

        if (config.isAddTimestamps()) {
            result = ConsolidateDataHelper.addTimestampColumns(result);
        }
        result = trimResult(result, config.getTrimFields());
        return result;
    }

    private Node trimResult(Node source, List<String> trimFields) {
        if (CollectionUtils.isNotEmpty(trimFields)) {
            Set<String> existingFields = new HashSet<>(source.getFieldNames());
            for (String trimField : trimFields) {
                if (CollectionUtils.isNotEmpty(existingFields) && existingFields.contains(trimField)) {
                    source = source.apply(new TrimFunction(trimField), new FieldList(trimField),
                            new FieldMetadata(trimField, String.class));
                }
            }
        }
        return source;
    }

    private void addIdColumn(List<Node> sources, String idColumn) {
        for (int i = 0; i < sources.size(); i++) {
            Node node = sources.get(i);
            if (!node.getFieldNames().contains(idColumn)) {
                node = node.addUUID(idColumn);
                sources.set(i, node);
            }
        }
    }

    private void dedupeSource(ConsolidateDataTransformerConfig config, List<Node> sources, String groupByKey) {
        if (!config.isDedupeSource()) {
            return;
        }
        for (int i = 0; i < sources.size(); i++) {
            Node source = sources.get(i);
            if (StringUtils.isEmpty(groupByKey) || !source.getFieldNames().contains(groupByKey)) {
                continue;
            }
            Node newSource = source.groupByAndLimit(new FieldList(groupByKey), 1);
            sources.set(i, newSource);
        }
    }

    private void cloneSrcFlds(List<Node> sources, String[][] cloneSrcFlds) {
        if (cloneSrcFlds == null) {
            return;
        }
        for (int i = 0; i < sources.size(); i++) {
            Node source = sources.get(i);
            for (String[] fldPair : cloneSrcFlds) {
                FieldMetadata originFM = source.getSchema(fldPair[0]);
                if (originFM != null) {
                    FieldMetadata newFM = new FieldMetadata(fldPair[1], originFM.getJavaType());
                    source = source.apply(fldPair[0], new FieldList(fldPair[0]), newFM);
                }
            }
            sources.set(i, source);
        }
    }

    private void renameSrcFlds(List<Node> sources, String[][] renameSrcFlds) {
        if (renameSrcFlds == null) {
            return;
        }
        for (int i = 0; i < sources.size(); i++) {
            Node source = sources.get(i);
            List<String> previousFlds = new ArrayList<>();
            List<String> newFlds = new ArrayList<>();
            for (String[] fldPair : renameSrcFlds) {
                if (source.getSchema(fldPair[0]) != null) {
                    previousFlds.add(fldPair[0]);
                    newFlds.add(fldPair[1]);
                }
            }
            if (CollectionUtils.isNotEmpty(previousFlds)) {
                source = source.rename(new FieldList(previousFlds), new FieldList(newFlds));
                sources.set(i, source);
            }
        }
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return ConsolidateDataTransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return ConsolidateDataFlow.DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return ConsolidateDataFlow.TRANSFORMER_NAME;

    }

}
