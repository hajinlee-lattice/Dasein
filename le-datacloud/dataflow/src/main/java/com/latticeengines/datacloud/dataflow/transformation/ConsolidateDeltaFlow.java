package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.utils.LatticeAccountIdUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDeltaTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;

@Component("consolidateDeltaDataFlow")
public class ConsolidateDeltaFlow extends ConfigurableFlowBase<ConsolidateDeltaTransformerConfig> {

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        ConsolidateDeltaTransformerConfig config = getTransformerConfig(parameters);
        String srcId = config.getSrcIdField();
        String masterId = TableRoleInCollection.ConsolidatedAccount.getPrimaryKey().name();

        List<Node> sources = new ArrayList<>();
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
            sourceNames.add(sourceName);
        }
        if (sources.size() <= 1) {
            return sources.get(0);
        }
        if (sources.size() != 2) {
            throw new RuntimeException("There should be two tables: input and master table!");
        }

        Node idNode = sources.get(0).retain(new FieldList(masterId));
        Node masterNode = sources.get(1);
        List<String> fieldToRetain = masterNode.getFieldNames();

        Node result = idNode.leftJoin(new FieldList(masterId), masterNode, new FieldList(masterId));
        result = result.retain(new FieldList(fieldToRetain));
        return result;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return ConsolidateDeltaTransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "consolidateDeltaDataFlow";
    }

    @Override
    public String getTransformerName() {
        return "consolidateDeltaTransformer";

    }
}