package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.Arrays;
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

    private PeriodBuilder periodBuilder;
    private PeriodStrategy periodStrategy;

    public ConsolidateAddPeriodColumnFunction(PeriodStrategy periodStrategy, String trxDateColumn, String targetField) {
        super(new Fields(targetField));
        this.trxDateColumn = trxDateColumn;
        this.periodStrategy = periodStrategy;
        this.namePositionMap = getPositionMap(Arrays.asList(trxDateColumn));
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String trxDateStr = arguments.getString(namePositionMap.get(trxDateColumn));

        Integer result = getPeriodBuilder().toPeriodId(trxDateStr);
        functionCall.getOutputCollector().add(new Tuple(result));
    }

    private PeriodBuilder getPeriodBuilder() {
        if (periodBuilder == null) {
            periodBuilder = PeriodBuilderFactory.build(periodStrategy);
        }
        return periodBuilder;
    }

    private Map<String, Integer> getPositionMap(List<String> fields) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (String field : fields) {
            positionMap.put(field, pos++);
        }
        return positionMap;
    }

}
