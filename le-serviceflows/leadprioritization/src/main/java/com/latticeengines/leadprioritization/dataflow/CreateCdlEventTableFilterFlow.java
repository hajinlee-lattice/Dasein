package com.latticeengines.leadprioritization.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.dataflow.CreateCdlEventTableFilterParameters;

@Component("createCdlEventTableFilterFlow")
public class CreateCdlEventTableFilterFlow extends TypesafeDataFlowBuilder<CreateCdlEventTableFilterParameters> {

    private static final Logger log = LoggerFactory.getLogger(CreateCdlEventTableFilterFlow.class);

    @Override
    public Node construct(CreateCdlEventTableFilterParameters parameters) {
        List<String> retainFields = new ArrayList<>();
        Node trainFilterTable = addSource(parameters.trainFilterTable);
        Node targetFilterTable = addSource(parameters.targetFilterTable);
        retainFields.addAll(targetFilterTable.getFieldNames());
        String target = InterfaceName.Target.name();
        retainFields.add(InterfaceName.Train.name());
        retainFields.add(target);

        trainFilterTable = trainFilterTable.addColumnWithFixedValue(InterfaceName.Train.toString(), 1, Integer.class);
        targetFilterTable = targetFilterTable.addColumnWithFixedValue(InterfaceName.Target.toString(), 1,
                Integer.class);
        FieldList joinFields = new FieldList(InterfaceName.AccountId.name(), InterfaceName.PeriodId.name());
        Node result = trainFilterTable.leftJoin(joinFields, targetFilterTable, joinFields);

        result = result.apply(target + " == null ? new Integer(0) : " + target, new FieldList(target),
                new FieldMetadata(target, Integer.class));
        result = result.retain(new FieldList(retainFields));
        log.info("Cdl event table filter's columns=", retainFields);
        return result;
    }

}
