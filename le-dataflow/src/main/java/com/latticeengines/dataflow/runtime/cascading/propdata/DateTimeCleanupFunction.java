package com.latticeengines.dataflow.runtime.cascading.propdata;

import cascading.operation.Function;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.latticeengines.common.exposed.util.DateTimeUtils;

/**
 * This function removes clearly out of range timestamps
 */
@SuppressWarnings("rawtypes")
public class DateTimeCleanupFunction extends CleanupFunction implements Function {

    private static final long serialVersionUID = -8497965168553160455L;
    private String dateTimeField;

    public DateTimeCleanupFunction(String dateTimeField) {
        super(new Fields(dateTimeField), false); // do not remove records with
                                                 // invalid datetime.
        this.dateTimeField = dateTimeField;
    }

    @Override
    protected Tuple cleanupArguments(TupleEntry arguments) {
        try {
            Long dateTime = arguments.getLong(dateTimeField);
            if (DateTimeUtils.isInValidRange(dateTime)) {
                return new Tuple(dateTime);
            } else {
                return null;
            }
        } catch (Exception e) {
            return null;
        }
    }

}
