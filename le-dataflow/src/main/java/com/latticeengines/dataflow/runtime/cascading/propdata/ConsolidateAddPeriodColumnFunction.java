package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.cdl.PeriodBuilderFactory;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.period.PeriodBuilder;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings({ "rawtypes", "serial" })
public class ConsolidateAddPeriodColumnFunction extends BaseOperation implements Function {
    private Map<String, Integer> namePositionMap;
    private String trxDateColumn;
    private int periodNameLoc;
    private int periodIdLoc;

    // PeriodStrategy name -> PeriodBuilder
    private Map<String, PeriodBuilder> periodBuilders;

    public ConsolidateAddPeriodColumnFunction(Fields fieldDeclaration,
            List<PeriodStrategy> periodStrategies, String trxDateColumn, String periodNameField,
            String periodIdField) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
        this.trxDateColumn = trxDateColumn;
        this.periodNameLoc = this.namePositionMap.get(periodNameField);
        this.periodIdLoc = this.namePositionMap.get(periodIdField);
        periodBuilders = new HashMap<>();
        for (PeriodStrategy strategy : periodStrategies) {
            periodBuilders.put(strategy.getName(), PeriodBuilderFactory.build(strategy));
        }
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String trxDateStr = arguments.getString(trxDateColumn);

        for (Map.Entry<String, PeriodBuilder> ent : periodBuilders.entrySet()) {
            Integer periodId = ent.getValue().toPeriodId(trxDateStr);
            Tuple result = Tuple.size(getFieldDeclaration().size());
            result.set(periodNameLoc, ent.getKey());
            result.set(periodIdLoc, periodId);
            functionCall.getOutputCollector().add(result);

        }
    }

    private Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field : fieldDeclaration) {
            String fieldName = (String) field;
            positionMap.put(fieldName, pos++);
        }
        return positionMap;
    }

}
