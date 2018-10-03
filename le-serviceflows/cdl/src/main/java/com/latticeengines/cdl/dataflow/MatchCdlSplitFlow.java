package com.latticeengines.cdl.dataflow;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.MatchCdlSplitParameters;

@Component("matchCdlSplitFlow")
public class MatchCdlSplitFlow extends TypesafeDataFlowBuilder<MatchCdlSplitParameters> {

    @Override
    public Node construct(MatchCdlSplitParameters parameters) {
        Node inputTable = addSource(parameters.inputTable);
        Node result = inputTable.filter(parameters.expression, new FieldList(parameters.filterField));
        List<String> retainFields = parameters.retainFields;
        if (CollectionUtils.isNotEmpty(retainFields)) {
            retainFields = retainFields.stream().filter(f -> inputTable.getFieldNames().contains(f))
                    .collect(Collectors.toList());
            result = result.retain(new FieldList(retainFields));
        }
        return result;
    }

}
