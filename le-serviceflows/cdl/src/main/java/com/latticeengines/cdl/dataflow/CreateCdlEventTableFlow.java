package com.latticeengines.cdl.dataflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.CreateCdlEventTableParameters;

@Component("createCdlEventTableFlow")
public class CreateCdlEventTableFlow extends TypesafeDataFlowBuilder<CreateCdlEventTableParameters> {

    private static final Logger log = LoggerFactory.getLogger(CreateCdlEventTableFlow.class);

    @Override
    public Node construct(CreateCdlEventTableParameters parameters) {
        Node inputTable = addSource(parameters.inputTable);
        Node apsTable = addSource(parameters.apsTable);
        Node accountTable = addSource(parameters.accountTable);

        List<String> retainFields = buildRetainFields(inputTable, apsTable, accountTable);

        FieldList inputGroupFields = new FieldList(InterfaceName.AccountId.name(), InterfaceName.PeriodId.name());
        Node result = apsTable.join(new FieldList("LEAccount_ID", "Period_ID"), inputTable, inputGroupFields,
                JoinType.RIGHT);
        FieldList accountGroupFields = new FieldList(InterfaceName.AccountId.name());
        result = result.leftJoin(accountGroupFields, accountTable, accountGroupFields);

        result = result.retain(new FieldList(retainFields));
        log.info("Cdl event table's columns=", retainFields);
        return result;
    }

    private List<String> buildRetainFields(Node inputTable, Node apsTable, Node accountTable) {
        List<String> retainFields = new ArrayList<>();
        retainFields.addAll(apsTable.getFieldNames());
        retainFields.addAll(accountTable.getFieldNames());
        if (inputTable.getFieldNames().contains(InterfaceName.Train.name()))
            retainFields.add(InterfaceName.Train.name());
        if (inputTable.getFieldNames().contains(InterfaceName.Target.name()))
            retainFields.add(InterfaceName.Target.name());
        retainFields.removeAll(Arrays.asList(InterfaceName.AccountId.name(), InterfaceName.CDLCreatedTime.name(),
                InterfaceName.CDLUpdatedTime.name()));
        return retainFields;
    }

}
