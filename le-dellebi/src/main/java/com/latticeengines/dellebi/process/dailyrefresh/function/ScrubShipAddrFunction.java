package com.latticeengines.dellebi.process.dailyrefresh.function;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.mapred.FileSplit;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tap.hadoop.io.MultiInputSplit;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class ScrubShipAddrFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 7563216485842844330L;

    public ScrubShipAddrFunction(Fields fieldDeclaration) {
        super(2, fieldDeclaration);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry argument = functionCall.getArguments();

        HadoopFlowProcess hfp = (HadoopFlowProcess) flowProcess;
        MultiInputSplit mis = (MultiInputSplit) hfp.getReporter().getInputSplit();
        FileSplit fs = (FileSplit) mis.getWrappedInputSplit();
        String fileName = fs.getPath().getName();

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Calendar cal = Calendar.getInstance();

        String stageDate = dateFormat.format(cal.getTime());
        String processdFlg = "0";

        Tuple result = new Tuple();

        for (int i = 0; i < argument.size(); i++) {
            result.add(argument.getString(i));
        }

        // Add additional fields.
        result.add(processdFlg);
        result.add(stageDate);
        result.add(fileName);

        functionCall.getOutputCollector().add(result);
    }
}
