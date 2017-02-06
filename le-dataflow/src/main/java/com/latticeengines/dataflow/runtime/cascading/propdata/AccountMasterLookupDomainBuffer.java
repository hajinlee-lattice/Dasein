package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AccountMasterLookupDomainBuffer extends BaseOperation implements Buffer {
    private static final long serialVersionUID = 4217950767704131475L;

    protected Map<String, Integer> namePositionMap;
    private String dunsField;
    private String duDunsField;
    private String guDunsField;
    private String employeeField;
    private String salesVolumeField;
    private String isPrimaryLocationField;

    private List<String> returnedFields;
    private Map<String, Object> comparedData = new HashMap<>(); // Field name -> Value
    private Map<String, Object> returnedData = new HashMap<>(); // Field name -> Value

    private AccountMasterLookupDomainBuffer(Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
    }

    public AccountMasterLookupDomainBuffer(Fields fieldDeclaration, List<String> returnedFields, String dunsField,
            String duDunsField, String guDunsField, String employeeField, String salesVolumeField,
            String isPrimaryLocationField) {
        this(fieldDeclaration);
        this.dunsField = dunsField;
        this.duDunsField = duDunsField;
        this.guDunsField = guDunsField;
        this.employeeField = employeeField;
        this.salesVolumeField = salesVolumeField;
        this.returnedFields = returnedFields;
        this.isPrimaryLocationField = isPrimaryLocationField;
    }

    @SuppressWarnings("unchecked")
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        comparedData.clear();
        returnedData.clear();
        TupleEntry group = bufferCall.getGroup();
        Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
        setupTupleForArgument(bufferCall, arguments, group);
    }

    private Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field : fieldDeclaration) {
            String fieldName = (String) field;
            positionMap.put(fieldName, pos++);
        }
        return positionMap;
    }

    private void setupTupleForArgument(BufferCall bufferCall, Iterator<TupleEntry> argumentsInGroup, TupleEntry group) {
        while (argumentsInGroup.hasNext()) {
            TupleEntry arguments = argumentsInGroup.next();
            if (returnedData.size() == 0) {
                replaceComparedData(arguments);
                replaceReturnedData(arguments);
                continue;
            }
            int res = checkRuleStringIsNotNull(arguments, dunsField);
            if (res > 0) {
                replaceComparedData(arguments);
                replaceReturnedData(arguments);
                continue;
            } else if (res < 0) {
                continue;
            }
            res = checkRuleStringIsNotNull(arguments, duDunsField);
            if (res > 0) {
                replaceComparedData(arguments);
                replaceReturnedData(arguments);
                continue;
            } else if (res < 0) {
                continue;
            }
            res = checkRuleStringsEqual(arguments, dunsField, duDunsField);
            if (res > 0) {
                replaceComparedData(arguments);
                replaceReturnedData(arguments);
                continue;
            } else if (res < 0) {
                continue;
            }
            res = checkRuleStringsEqual(arguments, dunsField, guDunsField);
            if (res > 0) {
                replaceComparedData(arguments);
                replaceReturnedData(arguments);
                continue;
            } else if (res < 0) {
                continue;
            }
            res = checkRuleIntegersEqual(arguments, employeeField);
            if (res > 0) {
                replaceComparedData(arguments);
                replaceReturnedData(arguments);
                continue;
            } else if (res < 0) {
                continue;
            }
            res = checkRuleLongsEqual(arguments, salesVolumeField);
            if (res > 0) {
                replaceComparedData(arguments);
                replaceReturnedData(arguments);
                continue;
            } else if (res < 0) {
                continue;
            }
            res = checkRuleBooleanValuedStringIsTrue(arguments, isPrimaryLocationField);
            if (res > 0) {
                replaceComparedData(arguments);
                replaceReturnedData(arguments);
                continue;
            } else if (res < 0) {
                continue;
            }
        }
        if (returnedData.size() != returnedFields.size()) {
            throw new RuntimeException("No proper result is chosen");
        }
        Tuple result = Tuple.size(getFieldDeclaration().size());
        for (String field : returnedFields) {
            Integer loc = namePositionMap.get(field);
            result.set(loc, returnedData.get(field));
        }
        bufferCall.getOutputCollector().add(result);
    }

    private void replaceComparedData(TupleEntry arguments) {
        comparedData.put(dunsField, arguments.getString(dunsField));
        comparedData.put(duDunsField, arguments.getString(duDunsField));
        comparedData.put(guDunsField, arguments.getString(guDunsField));
        comparedData.put(employeeField, arguments.getInteger(employeeField));
        comparedData.put(salesVolumeField, arguments.getLong(salesVolumeField));
        comparedData.put(isPrimaryLocationField, arguments.getString(isPrimaryLocationField));
    }

    private void replaceReturnedData(TupleEntry arguments) {
        for (String field : returnedFields) {
            returnedData.put(field, arguments.getObject(field));
        }
    }

    private int checkRuleStringIsNotNull(TupleEntry arguments, String field) {
        String checking = arguments.getString(field);
        String checked = (String) comparedData.get(field);
        if (StringUtils.isNotEmpty(checking) && StringUtils.isEmpty(checked)) {
            return 1;
        } else if (StringUtils.isNotEmpty(checked) && StringUtils.isEmpty(checking)) {
            return -1;
        } else {
            return 0;
        }
    }

    private int checkRuleStringsEqual(TupleEntry arguments, String field1, String field2) {
        String checking1 = arguments.getString(field1);
        String checking2 = arguments.getString(field2);
        String checked1 = (String) comparedData.get(field1);
        String checked2 = (String) comparedData.get(field2);
        if (StringUtils.isNotEmpty(checking1) && StringUtils.isNotEmpty(checking2) && checking1.equals(checking2)
                && (StringUtils.isEmpty(checked1) || StringUtils.isEmpty(checked2) || !checked1.equals(checked2))) {
            return 1;
        } else if (StringUtils.isNotEmpty(checked1) && StringUtils.isNotEmpty(checked2) && checked1.equals(checked2)
                && (StringUtils.isEmpty(checking1) || StringUtils.isEmpty(checking2) || !checking1.equals(checking2))) {
            return -1;
        } else {
            return 0;
        }
    }

    private int checkRuleIntegersEqual(TupleEntry arguments, String field) {
        Integer checking = arguments.getInteger(field);
        Integer checked = (Integer) comparedData.get(field);
        if (checking != null && (checked == null || checking.intValue() > checked.intValue())) {
            return 1;
        } else if (checked != null && (checking == null || checked.intValue() > checking.intValue())) {
            return -1;
        } else {
            return 0;
        }
    }

    private int checkRuleLongsEqual(TupleEntry arguments, String field) {
        Long checking = arguments.getLong(field);
        Long checked = (Long) comparedData.get(field);
        if (checking != null && (checked == null || checking.longValue() > checked.longValue())) {
            return 1;
        } else if (checked != null && (checking == null || checked.longValue() > checking.longValue())) {
            return -1;
        } else {
            return 0;
        }
    }

    private int checkRuleBooleanValuedStringIsTrue(TupleEntry arguments, String field) {
        String checking = arguments.getString(field);
        String checked = (String) comparedData.get(field);
        if (StringUtils.isNotEmpty(checking)
                && (checking.equalsIgnoreCase("Y") || checking.equalsIgnoreCase("YES") || checking.equalsIgnoreCase("1")
                        || checking.equalsIgnoreCase("TRUE"))
                && (StringUtils.isEmpty(checked) || (!checked.equalsIgnoreCase("Y") && !checked.equalsIgnoreCase("YES")
                        && !checked.equalsIgnoreCase("1") && !checked.equalsIgnoreCase("TRUE")))) {
            return 1;
        } else if (StringUtils.isNotEmpty(checked)
                && (checked.equalsIgnoreCase("Y") || checked.equalsIgnoreCase("YES") || checked.equalsIgnoreCase("1")
                        || checked.equalsIgnoreCase("TRUE"))
                && (StringUtils.isEmpty(checking)
                        || (!checking.equalsIgnoreCase("Y") && !checking.equalsIgnoreCase("YES")
                                && !checking.equalsIgnoreCase("1") && !checking.equalsIgnoreCase("TRUE")))) {
            return -1;
        } else {
            return 0;
        }
        
    }

}
