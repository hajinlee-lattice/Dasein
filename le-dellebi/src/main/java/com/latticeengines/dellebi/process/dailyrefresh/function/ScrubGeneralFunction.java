package com.latticeengines.dellebi.process.dailyrefresh.function;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.mapred.FileSplit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
public class ScrubGeneralFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = -3364175814238773294L;

    private static final Log log = LogFactory.getLog(ScrubGeneralFunction.class);

    public ScrubGeneralFunction(Fields fieldDeclaration) {
        super(2, fieldDeclaration);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {

        TupleEntry argument = functionCall.getArguments();

        HadoopFlowProcess hfp = (HadoopFlowProcess) flowProcess;
        MultiInputSplit mis = (MultiInputSplit) hfp.getReporter().getInputSplit();
        FileSplit fs = (FileSplit) mis.getWrappedInputSplit();
        String fileName = fs.getPath().getName();

        Tuple result = new Tuple();

        String stage_date = convertDateToString();
        String stage_dttm = convertDatetimeToString();

        int sizeOfFieldDeclaration = this.fieldDeclaration.size();

        for (int i = 0; i < sizeOfFieldDeclaration; i++) {
            String fieldName = this.fieldDeclaration.get(i).toString();
            switch (fieldName) {
            case "QUOTE_CREATE_DATE":
                String quoteCreationDate = convertDatetimeToDate(
                        argument.getString("QUOTE_CREATE_DATE"));
                result.add(quoteCreationDate);
                break;
            case "STAGE_DT":
                result.add(stage_date);
                break;
            case "STAGE_DTTM":
                result.add(stage_dttm);
                break;
            case "fileName":
                result.add(fileName);
                break;
            case "PROCESSED_FLG":
                result.add("0");
                break;
            case "isActive":
                result.add("1");
                break;
            case "ORD_DT":
                String ordDate = convertDatetimeToDate(argument.getString("ORD_DT"));
                result.add(ordDate);
                break;
            case "INV_DT":
                String invDate = convertDatetimeToDate(argument.getString("INV_DT"));
                result.add(invDate);
                break;
            case "ORD_STAT_DT":
                String ordStateDate = convertDatetimeToDate(argument.getString("ORD_STAT_DT"));
                result.add(ordStateDate);
                break;
            case "ORD_STAT_DTTM":
                String ordStateDttm = convertDatetimeToSQLDatetime(argument.getString("ORD_STAT_DTTM"));
                result.add(ordStateDttm);
                break;
            case "#ACTUAL_DATE":
                String actDate = convertDateToSQLDate(argument.getString("#ACTUAL_DATE"));
                result.add(actDate);
                break;

            default:
                String fieldValue = argument.getString(fieldName);
                if (fieldValue.contains(",")) {
                    result.add('"' + argument.getString(fieldName) + '"');
                } else {
                    result.add(argument.getString(fieldName));
                }
            }
        }

        functionCall.getOutputCollector().add(result);
    }

    private String convertDateToString() {
        SimpleDateFormat formatterNew = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date();

        String s = formatterNew.format(date);

        return s;
    }
    
    private String convertDatetimeToString() {
        SimpleDateFormat formatterNew = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date date = new Date();

        String s = formatterNew.format(date);

        return s;
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

    private String convertDatetimeToSQLDatetime(String s) {

        if (s != null && !s.isEmpty() && s.trim().length() > 10) {
            SimpleDateFormat formatterOld = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss");
            SimpleDateFormat formatterNew = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            Date date;
            try {
                date = formatterOld.parse(s);
                s = formatterNew.format(date);
            } catch (ParseException e) {
                log.error("Failed to convert " + s + " to SQL Datetime.");
                log.error("Failed!", e);
            }

        } else {
            return "";
        }
        return s;
    }

    private String convertDateToSQLDate(String s) {

        if (s != null && !s.isEmpty()) {
            SimpleDateFormat formatterOld = new SimpleDateFormat("MM/dd/yyyy");
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
