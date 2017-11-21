package com.latticeengines.dataflow.runtime.cascading.propdata;

import com.latticeengines.common.exposed.util.DateTimeUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings({ "rawtypes", "serial" })
public class ConsolidateAddDateColumnFuction extends BaseOperation implements Function {
    private String trxTimeColumn;

    public ConsolidateAddDateColumnFuction(String trxTimeColumn, String dateField, String datePeriodField) {
        super(new Fields(dateField, datePeriodField));
        this.trxTimeColumn = trxTimeColumn;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String trxTime = arguments.getString(trxTimeColumn).toString();
        String date = DateTimeUtils.toDateOnlyFromMillis(trxTime);
        Integer datePeriod = DateTimeUtils.dateToDayPeriod(date);
        functionCall.getOutputCollector().add(new Tuple(date, datePeriod));
    }

}
