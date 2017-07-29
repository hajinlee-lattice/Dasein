package com.latticeengines.dataflow.runtime.cascading.propdata;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.operation.Function;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class EmployeeCleanUpFunction extends CleanupFunction implements Function {

    private static final Logger log = LoggerFactory.getLogger(EmployeeCleanUpFunction.class);

    private static final long serialVersionUID = 3183542024951504507L;
    private String totalEmpField;

    public EmployeeCleanUpFunction(String totalEmpField) {
        super(new Fields(totalEmpField), false);
        this.totalEmpField = totalEmpField;
    }

    @Override
    protected Tuple cleanupArguments(TupleEntry arguments) {
        try {
            String numOfEmpVal = arguments.getString(this.totalEmpField);
            String result = numOfEmpVal.replaceAll("[^\\d->]", "");
            if (StringUtils.isBlank(result)) {
                return null;
            }
            Integer resultValue = Integer.parseInt(result);
            return new Tuple(resultValue);
        } catch (Exception e) {
            log.error("Error in cleaning up arguments", e);
            return null;
        }
    }

}
