package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.text.SimpleDateFormat;

import cascading.operation.Function;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class DateToTimestampFunction extends CleanupFunction implements Function {

    public static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static final long serialVersionUID = -1502335567861893433L;
    private String dateField;

    public DateToTimestampFunction(String dateField) {
        super(new Fields(dateField), true);
        this.dateField = dateField;
    }

    @Override
    protected Tuple cleanupArguments(TupleEntry arguments) {
        String date = arguments.getString(dateField);
        try {
            return new Tuple(dateFormat.parse(date).getTime());
        } catch (Exception e) {
            return null;
        }
    }

}
