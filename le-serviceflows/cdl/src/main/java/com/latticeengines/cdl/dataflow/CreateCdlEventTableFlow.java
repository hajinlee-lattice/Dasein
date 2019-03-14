package com.latticeengines.cdl.dataflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.CreateCdlEventTableParameters;

@Component("createCdlEventTableFlow")
public class CreateCdlEventTableFlow extends TypesafeDataFlowBuilder<CreateCdlEventTableParameters> {

    private static final Logger log = LoggerFactory.getLogger(CreateCdlEventTableFlow.class);

    @Override
    public Node construct(CreateCdlEventTableParameters parameters) {
        Node accountTable = addSource(parameters.accountTable);
        // this is a hack solution to solve the CompanyName column issue
        if (!accountTable.getFieldNames().contains(InterfaceName.CompanyName.name())) {
            for (String field : accountTable.getFieldNames()) {
                if (field.equalsIgnoreCase("name")) {
                    accountTable = accountTable.rename(new FieldList(field),
                            new FieldList(InterfaceName.CompanyName.name()));
                    accountTable = accountTable.retain(new FieldList(accountTable.getFieldNames()));
                    break;
                }
            }
        }

        Node inputTable = addSource(parameters.inputTable);

        Node apsTable = null;
        if (inputTable.getFieldNames().contains(InterfaceName.PeriodId.name()) && parameters.apsTable != null) {
            apsTable = addSource(parameters.apsTable);
        }

        List<String> retainFields = buildRetainFields(parameters, inputTable, apsTable, accountTable);

        FieldList accountGroupFields = new FieldList(InterfaceName.AccountId.name());
        Node result = inputTable.leftJoin(accountGroupFields, accountTable, accountGroupFields);

        if (apsTable != null) {
            FieldList inputGroupFields = new FieldList(InterfaceName.AccountId.name(), InterfaceName.PeriodId.name());
            result = apsTable.join(new FieldList(InterfaceName.LEAccount_ID.name(), InterfaceName.Period_ID.name()),
                    result, inputGroupFields, JoinType.RIGHT);
        }

        result = result.retain(new FieldList(retainFields));
        log.info("Cdl event table's columns=" + StringUtils.join(retainFields, ","));
        return result;
    }

    private List<String> buildRetainFields(CreateCdlEventTableParameters parameters, Node inputTable, Node apsTable,
            Node accountTable) {
        List<String> retainFields = new ArrayList<>();
        if (apsTable != null) {
            retainFields.addAll(apsTable.getFieldNames());
        }
        accountTable.getFieldNames().forEach(attr -> {
            if (!retainFields.contains(attr)) {
                retainFields.add(attr);
            }
        });
        potentialFieldsToRetain(parameters).forEach(attr -> {
            if (inputTable.getFieldNames().contains(attr) && !retainFields.contains(attr))
                retainFields.add(attr);
        });
        retainFields.removeAll(Arrays.asList(InterfaceName.CDLCreatedTime.name(), InterfaceName.CDLUpdatedTime.name()));
        return retainFields;
    }

    private Collection<String> potentialFieldsToRetain(CreateCdlEventTableParameters parameters) {
        return Arrays.asList( //
                parameters.eventColumn, //
                InterfaceName.Train.name(), //
                InterfaceName.__Revenue.name(), //
                ScoreResultField.ModelId.displayName, //
                InterfaceName.__Composite_Key__.name() //
        );
    }

}
