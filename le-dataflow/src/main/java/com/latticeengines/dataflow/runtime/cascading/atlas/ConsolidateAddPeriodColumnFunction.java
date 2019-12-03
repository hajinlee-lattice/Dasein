package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.dataflow.runtime.cascading.BaseFunction;
import com.latticeengines.domain.exposed.cdl.PeriodBuilderFactory;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.period.PeriodBuilder;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class ConsolidateAddPeriodColumnFunction extends BaseFunction {
    private String trxDateColumn;
    private int periodNameLoc;
    private int periodIdLoc;

    // Period name -> PeriodBuilder
    private Map<String, PeriodBuilder> periodBuilders;
    private Map<String, PeriodBuilder> naturalPeriodBuilders;

    public ConsolidateAddPeriodColumnFunction(Fields fieldDeclaration,
            List<PeriodStrategy> periodStrategies, String trxDateColumn, String periodNameField,
            String periodIdField) {
        super(fieldDeclaration);
        this.trxDateColumn = trxDateColumn;
        this.periodNameLoc = this.namePositionMap.get(periodNameField);
        this.periodIdLoc = this.namePositionMap.get(periodIdField);
        periodBuilders = new HashMap<>();
        naturalPeriodBuilders = new HashMap<>();
        for (PeriodStrategy strategy : periodStrategies) {
            periodBuilders.put(strategy.getName(), PeriodBuilderFactory.build(strategy));
            naturalPeriodBuilders.put(strategy.getName(), PeriodBuilderFactory.build(PeriodStrategy.NATURAL_PERIODS
                    .stream().filter(ns -> ns.getName().equals(strategy.getName())).findAny().get()));
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String trxDateStr = arguments.getString(trxDateColumn);

        // For transaction with Spending product type, always use natural
        // calendar even if business calendar is configured (Required by PM in
        // M25)
        if (ProductType.Spending.name().equals(arguments.getString(InterfaceName.ProductType.name()))) {
            for (Map.Entry<String, PeriodBuilder> ent : naturalPeriodBuilders.entrySet()) {
                functionCall.getOutputCollector().add(createResultTuple(trxDateStr, ent.getKey(), ent.getValue()));
            }
        } else {
            for (Map.Entry<String, PeriodBuilder> ent : periodBuilders.entrySet()) {
                functionCall.getOutputCollector().add(createResultTuple(trxDateStr, ent.getKey(), ent.getValue()));
            }
        }
    }

    private Tuple createResultTuple(String trxDateStr, String periodName, PeriodBuilder builder) {
        Integer periodId = builder.toPeriodId(trxDateStr);
        Tuple result = Tuple.size(getFieldDeclaration().size());
        result.set(periodNameLoc, periodName);
        result.set(periodIdLoc, periodId);
        return result;
    }

}
