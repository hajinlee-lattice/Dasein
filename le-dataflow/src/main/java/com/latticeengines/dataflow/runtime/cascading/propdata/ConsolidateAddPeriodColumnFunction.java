package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.period.CalendarMonthPeriodBuilder;
import com.latticeengines.common.exposed.period.PeriodBuilder;
import com.latticeengines.common.exposed.period.PeriodStrategy;

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
    private String minColumn;

    private PeriodBuilder periodBuilder;

    public ConsolidateAddPeriodColumnFunction(PeriodStrategy periodStrategy, String trxDateColumn, String minColumn,
                                              String targetField) {
        super(new Fields(targetField));
        this.trxDateColumn = trxDateColumn;
        this.minColumn = minColumn;
        this.namePositionMap = getPositionMap(Arrays.asList(trxDateColumn, minColumn));
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String minDateStr = arguments.getString(namePositionMap.get(minColumn));
        String trxDateStr = arguments.getString(namePositionMap.get(trxDateColumn));

        Integer result = getPeriodBuilder(minDateStr).toPeriodId(trxDateStr);
        functionCall.getOutputCollector().add(new Tuple(result));
    }

    private PeriodBuilder getPeriodBuilder(String minDateStr) {
        if (periodBuilder == null) {
            periodBuilder = new CalendarMonthPeriodBuilder(minDateStr);
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
