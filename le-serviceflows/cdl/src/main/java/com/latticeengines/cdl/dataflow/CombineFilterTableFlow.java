package com.latticeengines.cdl.dataflow;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CombineFilterTableParameters;

@Component("combineFilterTableFlow")
public class CombineFilterTableFlow extends TypesafeDataFlowBuilder<CombineFilterTableParameters> {

    @Override
    public Node construct(CombineFilterTableParameters parameters) {
        Node crossSellInputTable = null;
        if (StringUtils.isNotBlank(parameters.crossSellInputTable)) {
            crossSellInputTable = addSource(parameters.crossSellInputTable);
        }
        Node customEventInputTable = null;
        if (StringUtils.isNotBlank(parameters.customEventInputTable)) {
            customEventInputTable = addSource(parameters.customEventInputTable);
        }
        Node inputTable = null;
        if (crossSellInputTable != null) {
            inputTable = crossSellInputTable;
        }
        if (customEventInputTable != null) {
            if (inputTable != null) {
                customEventInputTable = customEventInputTable.addColumnWithFixedValue(InterfaceName.PeriodId.name(),
                        null, Long.class);
                customEventInputTable = customEventInputTable.retain(new FieldList(inputTable.getFieldNames()));
                inputTable = inputTable.merge(customEventInputTable);
            } else {
                inputTable = customEventInputTable;
            }
        }

        if (inputTable == null) {
            throw new IllegalStateException("Must provide either crossSellInputTable or customEventInputTable");
        }

        return inputTable;
    }

}
