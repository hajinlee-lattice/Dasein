package com.latticeengines.cdl.dataflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.MatchCdlAccountParameters;

@Component("matchCdlAccountFlow")
public class MatchCdlAccountFlow extends TypesafeDataFlowBuilder<MatchCdlAccountParameters> {

    private static final Logger log = LoggerFactory.getLogger(MatchCdlAccountFlow.class);

    @Override
    public Node construct(MatchCdlAccountParameters parameters) {
        Node inputTable = addSource(parameters.inputTable);
        Node accountTable = addSource(parameters.accountTable);

        List<String> retainFields = buildRetainFields(parameters, inputTable, accountTable);
        FieldList groupByFields = new FieldList(parameters.getMatchFields());
        Node result = inputTable.join(groupByFields, accountTable, groupByFields, JoinType.LEFT);
        if (parameters.isDedupe()) {
            result = result.groupByAndLimit(groupByFields, new FieldList(InterfaceName.CDLUpdatedTime.name()), 1, true,
                    true);
        }
        result = result.retain(new FieldList(retainFields));
        log.info("Match Cdl Account table's columns=" + StringUtils.join(retainFields, ","));
        return result;
    }

    private List<String> buildRetainFields(MatchCdlAccountParameters parameters, Node inputTable, Node accountTable) {
        List<String> retainFields = new ArrayList<>(inputTable.getFieldNames());
        accountTable.getFieldNames().forEach(attr -> {
            if (!retainFields.contains(attr))
                retainFields.add(attr);
        });
        retainFields.removeAll(Arrays.asList(InterfaceName.CDLCreatedTime.name(), InterfaceName.CDLUpdatedTime.name()));
        return retainFields;
    }

}
