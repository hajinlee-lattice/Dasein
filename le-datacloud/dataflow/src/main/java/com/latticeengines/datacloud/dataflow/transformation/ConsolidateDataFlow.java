package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.utils.LatticeAccountIdUtils;
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
public class ConsolidateDataFlow extends ConfigurableFlowBase<ConsolidateDataTransformerConfig> {

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        ConsolidateDataTransformerConfig config = getTransformerConfig(parameters);

        String srcId = config.getSrcIdField();
        String masterId = config.getMasterIdField();

        List<Node> sources = new ArrayList<>();
        List<Table> sourceTables = new ArrayList<>();
        List<String> sourceNames = new ArrayList<>();
        for (int i = 0; i < parameters.getBaseTables().size(); i++) {
            String sourceName = parameters.getBaseTables().get(i);
            Node source = addSource(sourceName);
            source = LatticeAccountIdUtils.convetLatticeAccountIdDataType(source);
            List<String> srcFields = source.getFieldNames();
            if (srcFields.contains(srcId) && !srcFields.contains(masterId)) {
                source = source.rename(new FieldList(srcId), new FieldList(masterId));
            }
            sources.add(source);
            sourceTables.add(getSourceMetadata(sourceName));
            sourceNames.add(sourceName);
        }
        if (sources.size() <= 1) {
            return sources.get(0);
        }

        Map<String, Map<String, String>> dupeFieldMap = new LinkedHashMap<>();
        List<String> fieldToRetain = new ArrayList<>();
        Set<String> commonFields = new HashSet<>();
        ConsolidateDataHelper consolidateHelper = new ConsolidateDataHelper();
        consolidateHelper.preProcessSources(sourceNames, sources, dupeFieldMap, fieldToRetain, commonFields);

        List<FieldList> groupFieldLists = consolidateHelper.getGroupFieldList(sourceNames, sourceTables, dupeFieldMap,
                masterId);

        Node result = sources.get(0).coGroup(groupFieldLists.get(0), sources.subList(1, sources.size()),
                groupFieldLists.subList(1, groupFieldLists.size()), JoinType.OUTER);

        List<String> allFieldNames = result.getFieldNames();
        Function<?> function = new ConsolidateDataFuction(allFieldNames, commonFields, dupeFieldMap);
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
}
