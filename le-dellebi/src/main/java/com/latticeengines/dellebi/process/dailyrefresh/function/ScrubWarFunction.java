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
public class ScrubWarFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 9034085248253118338L;

    public ScrubWarFunction(Fields fieldDeclaration) {
        super(2, fieldDeclaration);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry inputargument = functionCall.getArguments();

        HadoopFlowProcess hfp = (HadoopFlowProcess) flowProcess;
        MultiInputSplit mis = (MultiInputSplit) hfp.getReporter().getInputSplit();
        FileSplit fs = (FileSplit) mis.getWrappedInputSplit();
        String fileName = fs.getPath().getName();

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Calendar cal = Calendar.getInstance();

        String stageDate = dateFormat.format(cal.getTime());
        String processdFlg = "0";

        Tuple result = new Tuple();

        // All the fields be transformed and other fields to be added to the
        // result.
        for (int i = 0; i < inputargument.size(); i++) {
            result.add(inputargument.getString(i));
        }

        // Add additional fields.
        result.add(stageDate);
        result.add(processdFlg);
        result.add(fileName);

        functionCall.getOutputCollector().add(result);
    }
}
