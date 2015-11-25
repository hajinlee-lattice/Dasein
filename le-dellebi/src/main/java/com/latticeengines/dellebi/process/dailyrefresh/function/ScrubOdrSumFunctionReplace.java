package com.latticeengines.dellebi.process.dailyrefresh.function;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class ScrubOdrSumFunctionReplace extends BaseOperation implements Function {

    private static final long serialVersionUID = 7208027092973311661L;
    private static final Log log = LogFactory.getLog(ScrubOdrSumFunctionReplace.class);

    public ScrubOdrSumFunctionReplace(Fields fieldDeclaration) {
        super(2, fieldDeclaration);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry argument = functionCall.getArguments();
        String srcLclChnlCd = scrub(argument.getString("SRC_LCL_CHNL_CD"));
        String refLclChnlCd = scrub(argument.getString("REF_LCL_CHNL_CD"));

        String cnclDate = convertDatetimeToDate(argument.getString("CNCL_DT"));
        String invDate = convertDatetimeToDate(argument.getString("INV_DT"));
        String ordDate = convertDatetimeToDate(argument.getString("ORD_DT"));
        String ordStatDate = convertDatetimeToDate(argument.getString("ORD_STAT_DT"));
        String shipDate = convertDatetimeToDate(argument.getString("SHIP_DT"));
        String exchDate = convertDatetimeToDate(argument.getString("EXCH_DT"));
        String shipByDate = convertDatetimeToDate(argument.getString("SHIP_BY_DT"));
        String prfOfDlvrDate = convertDatetimeToDate(argument.getString("PRF_OF_DLVR_DT"));
        String estdBusDlvrDate = convertDatetimeToDate(argument.getString("ESTD_BUS_DLVR_DT"));

        Tuple result = new Tuple();

        result.add(srcLclChnlCd);
        result.add(refLclChnlCd);
        result.add(cnclDate);
        result.add(invDate);
        result.add(ordDate);
        result.add(ordStatDate);
        result.add(shipDate);
        result.add(exchDate);
        result.add(shipByDate);
        result.add(prfOfDlvrDate);
        result.add(estdBusDlvrDate);

        functionCall.getOutputCollector().add(result);
    }

    private String scrub(String s) {
        String returnString = "";
        if (s != null && !s.isEmpty() && s.trim().length() > 5) {
            s.trim();
            returnString = s.substring(0, 5);
        }
        return returnString;
    }

    private String convertDatetimeToDate(String s) {
        if (s != null && !s.isEmpty() && s.trim().length() > 10) {
            SimpleDateFormat formatterOld = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss");
            SimpleDateFormat formatterNew = new SimpleDateFormat("yyyy-MM-dd");
            Date date;
            try {
                date = formatterOld.parse(s);
                s = formatterNew.format(date);
            } catch (ParseException e) {
                log.error("Failed to convert " + s + " to Date.");
                log.error("Failed!", e);
            }

        } else {
            return "";
        }
        return s;
    }
}
