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
    private String rootTypeField;
    private String treeNum;
    private String reasonType;
    private Long multLargeCompThreshold;
    private final static String HIGHER_SALES_VOLUME = "HIGHER_SALES_VOLUME";
    private final static String MULTIPLE_LARGE_COMPANY = "MULTIPLE_LARGE_COMPANY";
    private final static String HIGHER_EMP_TOTAL = "HIGHER_EMP_TOTAL";
    private final static String HIGHER_NUM_OF_LOC = "HIGHER_NUM_OF_LOC";

    public DomainRowSelectorAggregator(Fields fieldDeclaration, String groupByField, String domainField,
            String dunsField, String rootTypeField, String salesVolField,
            String totalEmpField, String numOfLocField, String treeNum, String reasonType,
            Long multLargeCompThreshold) {
        super(fieldDeclaration);
        this.salesVolField = salesVolField;
        this.totalEmpField = totalEmpField;
        this.numOfLocField = numOfLocField;
        this.groupByField = groupByField;
        this.domainField = domainField;
        this.dunsField = dunsField;
        this.rootTypeField = rootTypeField;
        this.treeNum = treeNum;
        this.reasonType = reasonType;
        this.multLargeCompThreshold = multLargeCompThreshold;
    }

    public static class Context extends BaseAggregator.Context {
        Tuple result;
        Object domain = null;
        Object duns = null;
        Object rootType = null;
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
        int empTotalVal = Integer.valueOf(arguments.getString(totalEmpField));
        int numOfLocVal = arguments.getInteger(numOfLocField);
        int treeNumVal = arguments.getInteger(treeNum);
        if (salesVolVal > context.maxSalesVolume) {
            context.maxSalesVolume = salesVolVal;
            context.domain = arguments.getString(domainField);
            context.duns = arguments.getString(dunsField);
            context.rootType = arguments.getString(rootTypeField);
            context.treeNumber = treeNumVal;
            if (salesVolVal > multLargeCompThreshold)
                context.reasonType = MULTIPLE_LARGE_COMPANY;
            else
                context.reasonType = HIGHER_SALES_VOLUME;
            if (context.maxEmpTotal == 0 || (empTotalVal > context.maxEmpTotal)) {
                context.maxEmpTotal = empTotalVal;
                context.maxNumOfLoc = numOfLocVal;
            }
        } else if (salesVolVal.equals(context.maxSalesVolume)) {
            if (empTotalVal != context.maxEmpTotal) {
                if (empTotalVal > context.maxEmpTotal) {
                    context.maxEmpTotal = empTotalVal;
                    context.domain = arguments.getString(domainField);
                    context.duns = arguments.getString(dunsField);
                    context.rootType = arguments.getString(rootTypeField);
                    context.treeNumber = treeNumVal;
                }
                context.reasonType = HIGHER_EMP_TOTAL;
                if (context.maxNumOfLoc == 0 || (numOfLocVal > context.maxNumOfLoc))
                    context.maxNumOfLoc = numOfLocVal;
            } else if (empTotalVal == context.maxEmpTotal) {
                if (numOfLocVal != context.maxNumOfLoc) {
                    if (numOfLocVal > context.maxNumOfLoc) {
                        context.maxNumOfLoc = numOfLocVal;
                        context.domain = arguments.getString(domainField);
                        context.duns = arguments.getString(dunsField);
                        context.rootType = arguments.getString(rootTypeField);
                        context.treeNumber = treeNumVal;
                    }
                    context.reasonType = HIGHER_NUM_OF_LOC;
                }
            }
        }
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        context.result = new Tuple();
        context.result = Tuple.size(getFieldDeclaration().size());
        // setting tree number and domain
        context.result.set(0, context.domain);
        context.result.set(1, context.duns);
        context.result.set(2, context.rootType);
        context.result.set(3, context.treeNumber);
        context.result.set(4, context.reasonType);
        return context.result;
    }

}
