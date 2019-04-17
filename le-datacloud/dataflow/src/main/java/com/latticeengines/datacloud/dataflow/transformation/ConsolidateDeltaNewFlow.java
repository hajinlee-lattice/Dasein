package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;

/**
 * If only one input, directly return
 * Otherwise find the new rows in first input compared to the second.
 * Used by mergeNew step
 */
@Component("consolidateDeltaNewDataFlow")
public class ConsolidateDeltaNewFlow extends ConsolidateBaseFlow<ConsolidateDataTransformerConfig> {

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        ConsolidateDataTransformerConfig config = getTransformerConfig(parameters);

        List<Node> sources = new ArrayList<>();
        List<String> sourceNames = null;
        List<Table> sourceTables = null;
        String masterId = processIdColumns(parameters, config, sources, sourceTables, sourceNames);
        if (sources.size() <= 1) {
            return sources.get(0);
        }
        if (sources.size() != 2) {
            throw new RuntimeException("There should be two tables: input and master table!");
        }

        Node masterNode = sources.get(1);
        String latticeId = TableRoleInCollection.AccountMaster.getPrimaryKey().name();
        masterNode = masterNode.retain(new FieldList(masterId, latticeId));
        Node deltaNode = sources.get(0);
        List<String> fieldToRetain = deltaNode.getFieldNames();

        Node result = deltaNode.leftJoin(new FieldList(masterId), masterNode, new FieldList(masterId));
        result = result.filter(latticeId + " == null", new FieldList(latticeId));
        result = result.retain(new FieldList(fieldToRetain));
        return result;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return ConsolidateDataTransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "consolidateDeltaNewDataFlow";
    }

    @Override
    public String getTransformerName() {
        return "consolidateDeltaNewTransformer";

    }
}