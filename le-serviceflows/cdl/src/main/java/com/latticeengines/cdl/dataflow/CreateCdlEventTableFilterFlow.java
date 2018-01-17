package com.latticeengines.cdl.dataflow;

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
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.CreateCdlEventTableFilterParameters;

@Component("createCdlEventTableFilterFlow")
public class CreateCdlEventTableFilterFlow extends TypesafeDataFlowBuilder<CreateCdlEventTableFilterParameters> {

    private static final Logger log = LoggerFactory.getLogger(CreateCdlEventTableFilterFlow.class);

    @Override
    public Node construct(CreateCdlEventTableFilterParameters parameters) {
        List<String> retainFields = new ArrayList<>();
        Node trainFilterTable = addSource(parameters.trainFilterTable);
        Node eventFilterTable = addSource(parameters.eventFilterTable);

        retainFields.addAll(eventFilterTable.getFieldNames());
        String eventColumn = parameters.getEventColumn();
        retainFields.add(eventColumn);
        retainFields.add(InterfaceName.Train.name());
        String revenueColumn = InterfaceName.__Revenue.name();

        trainFilterTable = trainFilterTable.addColumnWithFixedValue(InterfaceName.Train.toString(), 1, Integer.class);
        eventFilterTable = eventFilterTable.addColumnWithFixedValue(eventColumn, 1, Integer.class);
        FieldList joinFields = new FieldList(InterfaceName.AccountId.name(), InterfaceName.PeriodId.name());
        Node result = trainFilterTable.leftJoin(joinFields, eventFilterTable, joinFields);

        result = result.apply(eventColumn + " == null ? new Integer(0) : " + eventColumn, new FieldList(eventColumn),
                new FieldMetadata(eventColumn, Integer.class));
        if (eventFilterTable.getFieldNames().contains(revenueColumn))
            result = result.apply(revenueColumn + " == null ? new Double(0) : " + revenueColumn,
                    new FieldList(revenueColumn), new FieldMetadata(revenueColumn, Double.class));
        result = result.retain(new FieldList(retainFields));
        log.info("Cdl event table filter's columns=", retainFields);
        return result;
    }

}
