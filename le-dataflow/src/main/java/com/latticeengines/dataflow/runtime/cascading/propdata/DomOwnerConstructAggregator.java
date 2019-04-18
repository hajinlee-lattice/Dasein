package com.latticeengines.dataflow.runtime.cascading.propdata;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class DomOwnerConstructAggregator extends BaseAggregator<DomOwnerConstructAggregator.Context>
        implements Aggregator<DomOwnerConstructAggregator.Context> {

    public static final String GOVERNMENT = "Government";
    public static final String EDUCATION = "Education";
    public static final String NON_PROFIT = "Non-profit";
    public static final Long NON_PROFIT_TOTAL_SALES = 1000000L;
    public static final Integer NON_PROFIT_TOTAL_EMP = 200;
    private static final long serialVersionUID = -4258093110031791835L;
    private static final String FRANCHISE = "FRANCHISE";
    private static final String HIGHER_SALES_VOLUME = "HIGHER_SALES_VOLUME";
    private static final String MULTIPLE_LARGE_COMPANY = "MULTIPLE_LARGE_COMPANY";
    private static final String HIGHER_EMP_TOTAL = "HIGHER_EMP_TOTAL";
    private static final String HIGHER_NUM_OF_LOC = "HIGHER_NUM_OF_LOC";
    private static final String MISSING_ROOT_DUNS = "MISSING_ROOT_DUNS";
    private static final String SINGLE_TREE = "SINGLE_TREE";
    private static final String OTHER = "OTHER";
    private String domainField = DataCloudConstants.AMS_ATTR_DOMAIN;
    private String rootDunsField;
    private String treeRootDunsField;
    private String dunsTypeField;
    private String salesVolField = DataCloudConstants.ATTR_SALES_VOL_US;
    private String totalEmpField = DataCloudConstants.ATTR_EMPLOYEE_TOTAL;
    private String numOfLocField = DataCloudConstants.ATTR_LE_NUMBER_OF_LOCATIONS;
    private String primIndustryField = DataCloudConstants.AMS_ATTR_PRIMARY_INDUSTRY;
    private Long multLargeCompThreshold;
    private int franchiseThreshold;

    public DomOwnerConstructAggregator(Fields fieldDeclaration, String rootDunsField,
            String treeRootDunsField, String dunsTypeField, Long multLargeCompThreshold,
            int franchiseThreshold) {
        super(fieldDeclaration);
        this.rootDunsField = rootDunsField;
        this.treeRootDunsField = treeRootDunsField;
        this.dunsTypeField = dunsTypeField;
        this.multLargeCompThreshold = multLargeCompThreshold;
        this.franchiseThreshold = franchiseThreshold;
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        Object grpObj = group.getObject(domainField);
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
        Long salesVol = (Long) arguments.getObject(salesVolField);
        String treeRootDunsVal = arguments.getString(treeRootDunsField);
        context.numOfTrees += 1;
        if (treeRootDunsVal == null) {
            return cleanup(context, MISSING_ROOT_DUNS);
        }
        if (context.numOfTrees > franchiseThreshold) {
            return cleanup(context, FRANCHISE);
        }
        if (salesVol != null && salesVol > multLargeCompThreshold) {
            context.numOfLargeComp += 1;
        }
        if (context.numOfLargeComp > 1) {
            return cleanup(context, MULTIPLE_LARGE_COMPANY);
        }
        if (context.numOfTrees == 1) {
            return update(context, arguments, SINGLE_TREE);
        }
        Integer empTotal = null;
        if (arguments.getString(totalEmpField) != null) {
            empTotal = Integer.parseInt(arguments.getString(totalEmpField));
        }
        Integer numOfLoc = (Integer) arguments.getObject(numOfLocField);
        int res = 0;
        if (!context.isNonProfitable && !isNoProfitable(arguments, salesVol, empTotal)) {
            res = checkRuleLargerLong(salesVol, context.salesVol);
            if (res > 0) {
                return update(context, arguments, HIGHER_SALES_VOLUME);
            } else if (res < 0) {
                return update(context, HIGHER_SALES_VOLUME);
            }
        }
        res = checkRuleLargerIntegers(empTotal, context.empTotal);
        if (res > 0) {
            return update(context, arguments, HIGHER_EMP_TOTAL);
        } else if (res < 0) {
            return update(context, HIGHER_EMP_TOTAL);
        }
        res = checkRuleLargerIntegers(numOfLoc, context.numOfLoc);
        if (res > 0) {
            return update(context, arguments, HIGHER_NUM_OF_LOC);
        } else if (res < 0) {
            return update(context, HIGHER_NUM_OF_LOC);
        }
        return cleanup(context, OTHER);
    }

    private boolean isNoProfitable(TupleEntry arguments, Long salesVolVal, Integer empTotalVal) {
        String primaryIndustry = arguments.getString(primIndustryField);
        if (salesVolVal == null)
            salesVolVal = 0L;
        if (empTotalVal == null)
            empTotalVal = 0;
        if (primaryIndustry != null && //
                (primaryIndustry.equals(GOVERNMENT) || primaryIndustry.equals(EDUCATION)
                        || primaryIndustry.equals(NON_PROFIT)) //
                || (salesVolVal < NON_PROFIT_TOTAL_SALES && empTotalVal > NON_PROFIT_TOTAL_EMP)) {
            return true;
        }
        return false;
    }

    private Context update(Context context, String updateReason) {
        if (context.reasonType.equals(SINGLE_TREE)) {
            context.reasonType = updateReason;
        }
        return context;
    }

    private Context update(Context context, TupleEntry arguments, String updateReason) {
        context.rootDuns = arguments.getString(rootDunsField);
        context.dunsType = arguments.getString(dunsTypeField);
        if (arguments.getObject(salesVolField) != null)
            context.salesVol = (Long) arguments.getObject(salesVolField);
        if (arguments.getString(totalEmpField) != null)
            context.empTotal = Integer.parseInt(arguments.getString(totalEmpField));
        if (arguments.getObject(numOfLocField) != null)
            context.numOfLoc = (Integer) arguments.getObject(numOfLocField);
        context.reasonType = updateReason;
        context.isNonProfitable = isNoProfitable(arguments, context.salesVol, context.empTotal);
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
        } else if (checked != null
                && (checking == null || checked.longValue() > checking.longValue())) {
            return -1;
        } else {
            return 0;
        }
    }

    private int checkRuleLargerIntegers(Integer checking, Integer checked) {
        if (checking != null && (checked == null || checking.intValue() > checked.intValue())) {
            return 1;
        } else if (checked != null
                && (checking == null || checked.intValue() > checking.intValue())) {
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

    public static class Context extends BaseAggregator.Context {
        int numOfTrees = 0;
        int numOfLargeComp = 0;
        String domain = null;
        String duns = null;
        String rootDuns = null;
        String dunsType = null;
        String reasonType = null;
        Long salesVol = 0L;
        int empTotal = 0;
        int numOfLoc = 0;
        boolean isNonProfitable = false;
        Tuple result;
    }
}
