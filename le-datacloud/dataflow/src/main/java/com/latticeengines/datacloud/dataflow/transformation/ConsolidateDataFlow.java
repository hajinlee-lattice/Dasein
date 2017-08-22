package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.Aggregation;
import com.latticeengines.dataflow.exposed.builder.common.AggregationType;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.ConsolidateDataFuction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.Table;

import cascading.flow.FlowDef;
import cascading.operation.Function;
import cascading.tuple.Fields;

@Component("consolidateDataFlow")
public class ConsolidateDataFlow extends ConsolidateBaseFlow<ConsolidateDataTransformerConfig> {

    private Node newRecordsCount;

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
        if (sources.size() <= 1) {
            return sources.get(0);
        }
        if (CollectionUtils.isNotEmpty(config.getCompositeKeys())) {
            groupByKey = buildNewIdColumn(config, sources);
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

        if (parameters.shouldCreateReport()) {
            String createDateInMasterTable = dupeFieldMap.get(sourceNames.get(sourceNames.size() - 1)).get(CREATE_DATE);
            newRecordsCount = result
                    .filter(createDateInMasterTable + " == null", new FieldList(createDateInMasterTable))
                    .groupBy(new FieldList(createDateInMasterTable),
                            Collections.singletonList(
                                    new Aggregation(createDateInMasterTable, "NewRecordsCount", AggregationType.COUNT)))
                    .renamePipe("new_records_count");
        }
        List<String> allFieldNames = result.getFieldNames();
        Function<?> function = new ConsolidateDataFuction(allFieldNames, commonFields, dupeFieldMap,
                config.getColumnsFromRight());
        result = result.apply(function, new FieldList(allFieldNames), getMetadata(result.getIdentifier()),
                new FieldList(allFieldNames), Fields.REPLACE);

        result = result.retain(new FieldList(fieldToRetain));
        return result;
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

    @Override
    protected FlowDef constructFlowDef() {
        FlowDef flowDef = super.constructFlowDef();
        if (newRecordsCount != null) {
            // flowDef.addTailSink(getPipeByIdentifier(newRecordsCount.getIdentifier()),
            // createSink(newRecordsCount.getIdentifier(), "/tmp/count"));
        }
        return flowDef;
    }
}
