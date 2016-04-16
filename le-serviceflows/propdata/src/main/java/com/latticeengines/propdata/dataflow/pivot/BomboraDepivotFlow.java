package com.latticeengines.propdata.dataflow.pivot;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.propdata.dataflow.PivotDataFlowParameters;

@Component("bomboraDepivotFlow")
public class BomboraDepivotFlow extends TypesafeDataFlowBuilder<PivotDataFlowParameters> {

    @Override
    public Node construct(PivotDataFlowParameters parameters) {
        // TODO - will develop this method in next txn

        // Node node = addSource("bomboraFirehose");
        // String[] targetFields = new String[] { "Topic", "Score" };
        // String[][] sourceFieldTuples = new String[][] { { "Topic1", "Score1"
        // }, { "Topic2", "Score2" },
        // { "Topic3", "Score3" }, { "Topic4", "Score4" } };
        // return node.depivot(targetFields, sourceFieldTuples);

        return null;
    }
}