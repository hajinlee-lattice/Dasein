package com.latticeengines.dataflow.runtime.cascading.propdata;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class DomainTreeCountRowSelectAggregator extends BaseAggregator<DomainTreeCountRowSelectAggregator.Context>
        implements Aggregator<DomainTreeCountRowSelectAggregator.Context> {

    private static final long serialVersionUID = -4258093110031791835L;
    private String groupbyField;
    private String domainField;
    private String rootDunsField;
    private String dunsTypeField;
    private String salesVolField;
    private String totalEmpField;
    private String numOfLocField;
    private String primIndustryField;
    private Long multLargeCompThreshold;
    private int franchiseThreshold;
    private final static String FRANCHISE = "FRANCHISE";
    private final static String HIGHER_SALES_VOLUME = "HIGHER_SALES_VOLUME";
    private final static String MULTIPLE_LARGE_COMPANY = "MULTIPLE_LARGE_COMPANY";
    private final static String HIGHER_EMP_TOTAL = "HIGHER_EMP_TOTAL";
    private final static String HIGHER_NUM_OF_LOC = "HIGHER_NUM_OF_LOC";
    private final static String OTHER = "OTHER";
    public final static String GOVERNMENT = "Government";
    public final static String EDUCATION = "Education";
    public final static String NON_PROFIT = "Non-profit";
    public final static Long NON_PROFIT_TOTAL_SALES = 1000000L;
    public final static Integer NON_PROFIT_TOTAL_EMP = 200;

    public DomainTreeCountRowSelectAggregator(Fields fieldDeclaration, String groupbyField, String domainField,
            String rootDunsField, String dunsTypeField, String salesVolField, String totalEmpField,
            String numOfLocField, String primIndustryField, Long multLargeCompThreshold, int franchiseThreshold) {
        super(fieldDeclaration);
        this.groupbyField = groupbyField;
        this.domainField = domainField;
        this.rootDunsField = rootDunsField;
        this.dunsTypeField = dunsTypeField;
        this.salesVolField = salesVolField;
        this.totalEmpField = totalEmpField;
        this.numOfLocField = numOfLocField;
        this.primIndustryField = primIndustryField;
        this.multLargeCompThreshold = multLargeCompThreshold;
        this.franchiseThreshold = franchiseThreshold;
    }

    public static class Context extends BaseAggregator.Context {
        int numOfTrees = 0;
        int numOfLargeComp = 0;
        String domain = null;
        String duns = null;
        String rootDuns = null;
        String dunsType = null;
        String reasonType = null;
        Long maxSalesVolume = 0L;
        int maxEmpTotal = 0;
        int maxNumOfLoc = 0;
        boolean isNonProfitable = false;
        Tuple result;
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        Object grpObj = group.getObject(groupbyField);
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
        Context context = new Context();
        context.domain = group.getString(domainField);
        return context;
    }

    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {
        Long salesVolVal = (Long) arguments.getObject(salesVolField);
        context.numOfTrees += 1;

        if (salesVolVal > multLargeCompThreshold) {
            context.numOfLargeComp += 1;
        }
        if (context.numOfTrees > franchiseThreshold) {
            return cleanup(context, FRANCHISE);
        }
        if (context.numOfLargeComp > 1) {
            return cleanup(context, MULTIPLE_LARGE_COMPANY);
        }
        Integer empTotal = null;
        if (arguments.getString(totalEmpField) != null) {
            empTotal = Integer.parseInt(arguments.getString(totalEmpField));
        }
        Integer numOfLocVal = (Integer) arguments.getObject(numOfLocField);
        if (context.rootDuns == null) {
            return update(context, arguments, OTHER);
        }
        int res = 0;
        if (!context.isNonProfitable && !isNoProfitable(arguments, salesVolVal, empTotal)) {
            res = checkRuleLargerLong(salesVolVal, context.maxSalesVolume);
            if (res > 0) {
                return update(context, arguments, HIGHER_SALES_VOLUME);
            } else if (res < 0) {
                return update(context, HIGHER_SALES_VOLUME);
            }
        }
        res = checkRuleLargerIntegers(empTotal, context.maxEmpTotal);
        if (res > 0) {
            return update(context, arguments, HIGHER_EMP_TOTAL);
        } else if (res < 0) {
            return update(context, HIGHER_EMP_TOTAL);
        }
        res = checkRuleLargerIntegers(numOfLocVal, context.maxNumOfLoc);
        if (res > 0) {
            return update(context, arguments, HIGHER_NUM_OF_LOC);
        } else if (res < 0) {
            return update(context, HIGHER_NUM_OF_LOC);
        }
        return context;
    }

    private boolean isNoProfitable(TupleEntry arguments, Long salesVolVal, Integer empTotalVal) {
        String primaryIndustry = arguments.getString(primIndustryField);
        if ((primaryIndustry.equals(GOVERNMENT) || primaryIndustry.equals(EDUCATION)
                || primaryIndustry.equals(NON_PROFIT))
                || (salesVolVal < NON_PROFIT_TOTAL_SALES && empTotalVal > NON_PROFIT_TOTAL_EMP)) {
            return true;
        }
        return false;
    }

    private Context update(Context context, String updateReason) {
        if (context.reasonType.equals(OTHER)) {
            context.reasonType = updateReason;
        }
        return context;
    }

    private Context update(Context context, TupleEntry arguments, String updateReason) {
        context.rootDuns = arguments.getString(rootDunsField);
        context.dunsType = arguments.getString(dunsTypeField);
        context.maxSalesVolume = (Long) arguments.getObject(salesVolField);
        if (arguments.getString(totalEmpField) != null)
            context.maxEmpTotal = Integer.parseInt(arguments.getString(totalEmpField));
        context.maxNumOfLoc = (Integer) arguments.getObject(numOfLocField);
        context.reasonType = updateReason;
        context.isNonProfitable = isNoProfitable(arguments, context.maxSalesVolume, context.maxEmpTotal);
        return context;
    }

    private Context cleanup(Context context, String reasonType) {
        context.rootDuns = null;
        context.dunsType = null;
        context.reasonType = reasonType;
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

    @Override
    protected Tuple finalizeContext(Context context) {
        context.result = new Tuple();
        context.result = Tuple.size(getFieldDeclaration().size());
        context.result.set(0, context.domain);
        context.result.set(1, context.rootDuns);
        context.result.set(2, context.dunsType);
        context.result.set(3, context.numOfTrees);
        context.result.set(4, context.reasonType);
        context.result.set(5, context.isNonProfitable);
        return context.result;
    }
}
