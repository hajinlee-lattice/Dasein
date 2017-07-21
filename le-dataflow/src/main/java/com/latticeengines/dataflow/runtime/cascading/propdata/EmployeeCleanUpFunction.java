package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cascading.operation.Function;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class EmployeeCleanUpFunction extends CleanupFunction implements Function {

    private static final Log log = LogFactory.getLog(EmployeeCleanUpFunction.class);
    private static Pattern totalEmployees = Pattern.compile("^\\s*(\\d+(\\s*,\\s*\\d+)*)?\\s*$");

    private static final long serialVersionUID = 3183542024951504507L;
    private String numOfEmp;

    public EmployeeCleanUpFunction(String numOfEmp) {
        super(new Fields(numOfEmp), true);
        this.numOfEmp = numOfEmp;
    }

    @Override
    protected Tuple cleanupArguments(TupleEntry arguments) {
        try {
            String numOfEmp = arguments.getString(this.numOfEmp);
            String result = "";
            Matcher matcher = totalEmployees.matcher(numOfEmp);
            if (matcher.find()) {
                if (numOfEmp.contains(",")) {
                    int index = numOfEmp.indexOf(",");
                    result = numOfEmp.substring(0, index) + numOfEmp.substring(index + 1);
                } else {
                    result = numOfEmp;
                }
            } else {
                result = null;
            }
            if (result == null || result.isEmpty()) {
                Integer intValue = null;
                return new Tuple(intValue);
            } else {
                Integer resultValue = Integer.parseInt(result);
                return new Tuple(resultValue);
            }
        } catch (Exception e) {
            log.error("Error in cleaning up arguments");
            return null;
        }
    }

}
