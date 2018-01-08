package com.latticeengines.dataflow.runtime.cascading.propdata;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class DomainRowSelectorAggregator extends BaseAggregator<DomainRowSelectorAggregator.Context>
        implements Aggregator<DomainRowSelectorAggregator.Context> {

    private static final long serialVersionUID = -7010468963852604265L;
    private String salesVolField;
    private String totalEmpField;
    private String numOfLocField;
    private String groupByField;
    private String domainField;
    private String dunsField;
    private String rootDunsField;
    private String dunsTypeField;
    private String treeNum;
    private String reasonType;
    private Long multLargeCompThreshold;
    private final static String HIGHER_SALES_VOLUME = "HIGHER_SALES_VOLUME";
    private final static String MULTIPLE_LARGE_COMPANY = "MULTIPLE_LARGE_COMPANY";
    private final static String HIGHER_EMP_TOTAL = "HIGHER_EMP_TOTAL";
    private final static String HIGHER_NUM_OF_LOC = "HIGHER_NUM_OF_LOC";

    public DomainRowSelectorAggregator(Fields fieldDeclaration, String groupByField, String domainField,
            String dunsField, String rootDunsField, String dunsTypeField, String salesVolField,
            String totalEmpField, String numOfLocField, String treeNum, String reasonType,
            Long multLargeCompThreshold) {
        super(fieldDeclaration);
        this.salesVolField = salesVolField;
        this.totalEmpField = totalEmpField;
        this.numOfLocField = numOfLocField;
        this.groupByField = groupByField;
        this.domainField = domainField;
        this.dunsField = dunsField;
        this.rootDunsField = rootDunsField;
        this.dunsTypeField = dunsTypeField;
        this.treeNum = treeNum;
        this.reasonType = reasonType;
        this.multLargeCompThreshold = multLargeCompThreshold;
    }

    public static class Context extends BaseAggregator.Context {
        Tuple result;
        Object domain = null;
        Object duns = null;
        Object rootDuns = null;
        Object dunsType = null;
        Long maxSalesVolume = 0L;
        int maxEmpTotal = 0;
        int maxNumOfLoc = 0;
        int treeNumber = 0;
        int count = 0;
        Object reasonType = null;
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        Object grpObj = group.getObject(groupByField);
        if (grpObj == null) {
            return true;
        }
        if (grpObj instanceof Utf8) {
            return StringUtils.isBlank(grpObj.toString());
        }
        if (grpObj instanceof String) {
            return StringUtils.isBlank((String) grpObj);
        }
        return false;
    }

    @Override
    protected Context initializeContext(TupleEntry group) {
        return new Context();
    }

    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {
        context.count++;
        Long salesVolVal = arguments.getLong(salesVolField);
        String empCount = arguments.getString(totalEmpField);
        int empTotalVal = 0;
        if (empCount != null)
            empTotalVal = Integer.valueOf(arguments.getString(totalEmpField));
        int numOfLocVal = arguments.getInteger(numOfLocField);
        int treeNumVal = arguments.getInteger(treeNum);
        int res = checkRuleLargerLong(salesVolVal, context.maxSalesVolume);
        if (res > 0) {
            context.maxSalesVolume = salesVolVal;
            context.treeNumber = treeNumVal;
            if (salesVolVal > multLargeCompThreshold)
                context.reasonType = MULTIPLE_LARGE_COMPANY;
            else
                context.reasonType = HIGHER_SALES_VOLUME;
            if (context.maxEmpTotal == 0 || (empTotalVal > context.maxEmpTotal)) {
                context.maxEmpTotal = empTotalVal;
                context.maxNumOfLoc = numOfLocVal;
            }
            return update(context, arguments);
        } else if (res == 0) {
            res = checkRuleLargerIntegers(empTotalVal, context.maxEmpTotal);
            if (res != 0) {
                if (res > 0) {
                    context.maxEmpTotal = empTotalVal;
                    context.treeNumber = treeNumVal;
                    update(context, arguments);
                }
                context.reasonType = HIGHER_EMP_TOTAL;
                if (context.maxNumOfLoc == 0 || (numOfLocVal > context.maxNumOfLoc))
                    context.maxNumOfLoc = numOfLocVal;
                return context;
            } else if (res == 0) {
                res = checkRuleLargerIntegers(numOfLocVal, context.maxNumOfLoc);
                if (res > 0) {
                    context.maxNumOfLoc = numOfLocVal;
                    context.treeNumber = treeNumVal;
                    update(context, arguments);
                }
                context.reasonType = HIGHER_NUM_OF_LOC;
                return context;
            }
        }
        return context;
    }

    private int checkRuleLargerLong(Long checking, Long checked) {
        if (checking != null && (checked == null || checking.longValue() > checked.longValue())) {
            return 1;
        } else if (checked != null && (checking == null || checked.longValue() > checking.longValue())) {
            return -1;
        } else {
            return 0;
        }
    }

    private int checkRuleLargerIntegers(Integer checking, Integer checked) {
        if (checking != null && (checked == null || checking.intValue() > checked.intValue())) {
            return 1;
        } else if (checked != null && (checking == null || checked.intValue() > checking.intValue())) {
            return -1;
        } else {
            return 0;
        }
    }

    private Context update(Context context, TupleEntry arguments) {
        context.domain = arguments.getString(domainField);
        context.duns = arguments.getString(dunsField);
        context.rootDuns = arguments.getString(rootDunsField);
        context.dunsType = arguments.getString(dunsTypeField);
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        context.result = new Tuple();
        context.result = Tuple.size(getFieldDeclaration().size());
        // setting tree number and domain
        context.result.set(0, context.domain);
        context.result.set(1, context.duns);
        context.result.set(2, context.rootDuns);
        context.result.set(3, context.dunsType);
        context.result.set(4, context.treeNumber);
        context.result.set(5, context.reasonType);
        return context.result;
    }

}
