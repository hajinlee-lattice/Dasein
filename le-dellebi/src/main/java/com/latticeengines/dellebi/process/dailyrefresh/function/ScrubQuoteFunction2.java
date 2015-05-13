package com.latticeengines.dellebi.process.dailyrefresh.function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class ScrubQuoteFunction2 extends BaseOperation implements Function {

    public ScrubQuoteFunction2(Fields fieldDeclaration) {
        super(2, fieldDeclaration);
    }

    private static final Log log = LogFactory.getLog(ScrubQuoteFunction2.class);

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry argument = functionCall.getArguments();

        /*
         * HadoopFlowProcess hfp = (HadoopFlowProcess) flowProcess;
         * MultiInputSplit mis = (MultiInputSplit)
         * hfp.getReporter().getInputSplit(); FileSplit fs = (FileSplit)
         * mis.getWrappedInputSplit(); String fileName = fs.getPath().getName();
         */

        log.info("Quote function 2.");
        log.info("Inserted value: " + argument.getString("#QTE_NUM_VAL") + " Badge: "
                + argument.getString("LEAD_SLS_REP_ASSOC_BDGE_NBR"));

        // String quoteCreationDate =
        // convertDatetimeToDate(argument.getString("QUOTE_CREATE_DATE"));

        Tuple result = new Tuple();
        result.add(argument.getString("#QTE_NUM_VAL"));
        result.add(argument.getString("QUOTE_CREATE_DATE"));
        result.add(argument.getString("SLDT_CUST_NUM_VAL"));
        result.add(argument.getString("ITM_NUM_VAL"));
        result.add(argument.getString("LEAD_SLS_REP_ASSOC_BDGE_NBR"));
        result.add(argument.getString("SYS_QTY"));
        result.add(argument.getString("REVN_USD_AMT"));
        result.add("test file name");

        functionCall.getOutputCollector().add(result);

    }

    // private String convertDatetimeToDate(String s) {
    // if (s != null && !s.isEmpty() && s.trim().length()>10){
    // SimpleDateFormat formatterOld = new
    // SimpleDateFormat("MM/dd/yyyy hh:mm:ss");
    // SimpleDateFormat formatterNew = new SimpleDateFormat("yyyy-MM-dd");
    // Date date;
    // try {
    // date = formatterOld.parse(s);
    // s = formatterNew.format(date);
    // } catch (ParseException e) {
    // log.error("Failed to convert " + s + " to Date.");
    // log.error("Failed!", e);
    // }
    //
    // }else{
    // return "";
    // }
    // return s;
    // }

}
