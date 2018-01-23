package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashSet;

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
            String rootDunsField, String dunsTypeField, String salesVolField,
            String totalEmpField, String numOfLocField, String treeNum, String reasonType,
            Long multLargeCompThreshold) {
        super(fieldDeclaration);
        this.salesVolField = salesVolField;
        this.totalEmpField = totalEmpField;
        this.numOfLocField = numOfLocField;
        this.groupByField = groupByField;
        this.domainField = domainField;
        this.rootDunsField = rootDunsField;
        this.dunsTypeField = dunsTypeField;
        this.treeNum = treeNum;
        this.reasonType = reasonType;
        this.multLargeCompThreshold = multLargeCompThreshold;
    }

    public static class Context extends BaseAggregator.Context {
        Tuple result;
        Object domain = null;
        Object rootDuns = null;
        Object dunsType = null;
        Long maxSalesVolume = 0L;
        int maxEmpTotal = 0;
        int maxNumOfLoc = 0;
        int treeNumber = 0;
        Object reasonType = null;
        int largeCompCount = 0;
        HashSet<Object> visitedRootDuns = new HashSet<Object>();
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
        Long salesVolVal = (Long) arguments.getObject(salesVolField);
        Integer empTotalVal = 0;
        if (empTotalVal != null)
            empTotalVal = Integer.parseInt(arguments.getString(totalEmpField));
        Integer numOfLocVal = (Integer) arguments.getObject(numOfLocField);
        Object rootDuns = arguments.getObject(rootDunsField);
        int treeNumVal = arguments.getInteger(treeNum);
        int res = checkRuleLargerLong(salesVolVal, context.maxSalesVolume);
        // if salesVolume is greater
        if (res > 0) {
            context.maxSalesVolume = salesVolVal;
            context.treeNumber = treeNumVal;
            if (!context.visitedRootDuns.contains(rootDuns) && salesVolVal > multLargeCompThreshold) {
                // checking if in a group there are more than one large sales
                // volume making it unable to select one of them
                if (context.largeCompCount > 0) {
                    context.reasonType = MULTIPLE_LARGE_COMPANY;
                } else {
                    context.largeCompCount += 1;
                    context.reasonType = HIGHER_SALES_VOLUME;
                }
            } else
                context.reasonType = HIGHER_SALES_VOLUME;
            if (context.maxEmpTotal == 0 || (empTotalVal > context.maxEmpTotal)) {
                context.maxEmpTotal = empTotalVal;
                context.maxNumOfLoc = numOfLocVal;
            }
            return update(context, arguments);
        } else if (res == 0) {
            // if salesVolume are equal then need to check empTotal
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
        context.visitedRootDuns.add(rootDuns);
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
        if (context.reasonType.equals(MULTIPLE_LARGE_COMPANY)) {
            context.result.set(1, null);
            context.result.set(2, null);
        } else {
            context.result.set(1, context.rootDuns);
            context.result.set(2, context.dunsType);
        }
        context.result.set(3, context.treeNumber);
        context.result.set(4, context.reasonType);
        return context.result;
    }

}
