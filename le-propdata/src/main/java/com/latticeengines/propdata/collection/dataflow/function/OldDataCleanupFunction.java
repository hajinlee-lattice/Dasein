package com.latticeengines.propdata.collection.dataflow.function;


import java.util.Date;

import com.latticeengines.propdata.core.dataflow.function.CleanupFunction;

import cascading.operation.Function;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class OldDataCleanupFunction extends CleanupFunction implements Function
{
    private static final long serialVersionUID = -4470533364538287281L;
    private String timestampField;
    private Date earliest;

    public OldDataCleanupFunction(String timestampField, Date earliest) {
        super(new Fields(timestampField), true);
        this.timestampField = timestampField;
        this.earliest = earliest;
    }

    protected Tuple cleanupArguments(TupleEntry arguments) {
        try {
            Date timestamp = new Date(arguments.getLong(timestampField));
            if (timestamp.before(earliest)) {
                return null;
            } else {
                return new Tuple(arguments.getLong(timestampField));
            }
        } catch (Exception e) {
            return null;
        }
    }

}
