package com.latticeengines.leadprioritization.dataflow;

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
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.dataflow.CreateCdlEventTableParameters;

@Component("createCdlEventTableFlow")
public class CreateCdlEventTableFlow extends TypesafeDataFlowBuilder<CreateCdlEventTableParameters> {

    private static final Logger log = LoggerFactory.getLogger(CreateCdlEventTableFlow.class);

    @Override
    public Node construct(CreateCdlEventTableParameters parameters) {
        List<String> retainFields = new ArrayList<>();
        Node inputTable = addSource(parameters.inputTable);
        Node apsTable = addSource(parameters.apsTable);
        Node accountTable = addSource(parameters.accountTable);

        retainFields.addAll(apsTable.getFieldNames());
        retainFields.addAll(accountTable.getFieldNames());
        retainFields.add(InterfaceName.Train.name());
        retainFields.add(InterfaceName.Target.name());
        retainFields.removeAll(Arrays.asList(InterfaceName.AccountId.name(), "CREATION_DATE", "UPATE_DATE",
                InterfaceName.LatticeAccountId.name()));

        FieldList inputGroupFields = new FieldList(InterfaceName.AccountId.name(), InterfaceName.PeriodId.name());
        Node result = apsTable.join(new FieldList("LEAccount_ID", "Period_ID"), inputTable, inputGroupFields,
                JoinType.RIGHT);
        FieldList accountGroupFields = new FieldList(InterfaceName.AccountId.name());
        result = result.leftJoin(accountGroupFields, accountTable, accountGroupFields);

        result = result.retain(new FieldList(retainFields));
        log.info("Cdl event table's columns=", retainFields);
        return result;
    }

}
