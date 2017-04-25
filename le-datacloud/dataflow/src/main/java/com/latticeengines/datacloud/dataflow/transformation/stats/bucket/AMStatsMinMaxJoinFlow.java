package com.latticeengines.datacloud.dataflow.transformation.stats.bucket;

import java.util.Arrays;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.AMStatsFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;

@Component("amStatsMinMaxJoinFlow")
public class AMStatsMinMaxJoinFlow extends AMStatsFlowBase {

    @Override
    public Node construct(AccountMasterStatsParameters parameters) {

        Node node = addSource(parameters.getBaseTables().get(0));

        Node minMaxNode = null;
        if (parameters.isNumericalBucketsRequired()) {
            minMaxNode = addSource(parameters.getBaseTables().get(1));
        }

        if (parameters.isNumericalBucketsRequired()) {
            node = joinWithMinMaxNode(node, minMaxNode);
        }

        return node;
    }

    private Node joinWithMinMaxNode(Node node, Node minMaxNode) {
        node.renamePipe("beginJoinWithMinMax");

        node = node.addColumnWithFixedValue(MIN_MAX_JOIN_FIELD, 0, Integer.class);

        minMaxNode = minMaxNode.rename(new FieldList(MIN_MAX_JOIN_FIELD), new FieldList(MIN_MAX_JOIN_FIELD_RENAMED));

        Node joinedWithMinMax = minMaxNode.hashJoin(new FieldList(MIN_MAX_JOIN_FIELD_RENAMED), //
                Arrays.asList(node), //
                Arrays.asList(new FieldList(MIN_MAX_JOIN_FIELD)), //
                JoinType.INNER).renamePipe("joinedWithMinMax");

        joinedWithMinMax = joinedWithMinMax.discard(new FieldList(MIN_MAX_JOIN_FIELD, MIN_MAX_JOIN_FIELD_RENAMED));

        return joinedWithMinMax;
    }
}
