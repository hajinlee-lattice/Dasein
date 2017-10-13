package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.ConsolidateDataFuction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.Table;

import cascading.operation.Function;
import cascading.tuple.Fields;

@Component("consolidateDataFlow")
public class ConsolidateDataFlow extends ConsolidateBaseFlow<ConsolidateDataTransformerConfig> {

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        ConsolidateDataTransformerConfig config = getTransformerConfig(parameters);

        List<Node> sources = new ArrayList<>();
        List<Table> sourceTables = new ArrayList<>();
        List<String> sourceNames = new ArrayList<>();
        String groupByKey = processIdColumns(parameters, config, sources, sourceTables, sourceNames);
        if (config.isCreateTimestampColumn()) {
            createTimestampColumns(config, sources);
        }
        if (CollectionUtils.isNotEmpty(config.getCompositeKeys())) {
            groupByKey = buildNewIdColumn(config, sources);
        }
        dedupeSource(config, sources, groupByKey);
        if (sources.size() <= 1) {
            return sources.get(0);
        }

        Map<String, Map<String, String>> dupeFieldMap = new LinkedHashMap<>();
        List<String> fieldToRetain = new ArrayList<>();
        Set<String> commonFields = new HashSet<>();
        ConsolidateDataHelper consolidateHelper = new ConsolidateDataHelper();
        consolidateHelper.preProcessSources(sourceNames, sources, dupeFieldMap, fieldToRetain, commonFields);

        List<FieldList> groupFieldLists = consolidateHelper.getGroupFieldList(sourceNames, sourceTables, dupeFieldMap,
                groupByKey);

        Node result = sources.get(0).coGroup(groupFieldLists.get(0), sources.subList(1, sources.size()),
                groupFieldLists.subList(1, groupFieldLists.size()), JoinType.OUTER);

        List<String> allFieldNames = result.getFieldNames();
        Function<?> function = new ConsolidateDataFuction(allFieldNames, commonFields, dupeFieldMap,
                config.getColumnsFromRight());
        result = result.apply(function, new FieldList(allFieldNames), getMetadata(result.getIdentifier()),
                new FieldList(allFieldNames), Fields.REPLACE);

        result = result.retain(new FieldList(fieldToRetain));
        return result;
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
