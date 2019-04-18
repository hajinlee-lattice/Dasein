package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.BulkMatchMergerTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component("bulkMatchMergerFlow")
public class BulkMatchMergerFlow extends ConfigurableFlowBase<BulkMatchMergerTransformerConfig> {

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        BulkMatchMergerTransformerConfig config = getTransformerConfig(parameters);
        Node inputSource = config.isReverse() ? addSource(parameters.getBaseTables().get(1))
                : addSource(parameters.getBaseTables().get(0));
        Node matchSource = config.isReverse() ? addSource(parameters.getBaseTables().get(0))
                : addSource(parameters.getBaseTables().get(1));

        FieldList joinFields = new FieldList(config.getJoinField());
        Node result = matchSource.join(joinFields, inputSource, joinFields, JoinType.RIGHT);
        List<String> retainFields = getRetainFields(inputSource, matchSource);
        result = result.retain(new FieldList(retainFields));
        return result;
    }

    private List<String> getRetainFields(Node inputSource, Node matchSource) {
        List<String> retainFields = new ArrayList<>(inputSource.getFieldNames());
        matchSource.getFieldNames().forEach(f -> {
            if (!retainFields.contains(f)) {
                retainFields.add(f);
            }
        });
        return retainFields;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return BulkMatchMergerTransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "bulkMatchMergerFlow";
    }

    @Override
    public String getTransformerName() {
        return "bulkMatchMergerTransformer";

    }
}
