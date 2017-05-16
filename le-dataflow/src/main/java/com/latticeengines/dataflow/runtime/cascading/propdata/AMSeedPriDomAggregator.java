package com.latticeengines.dataflow.runtime.cascading.propdata;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class AMSeedPriDomAggregator
        extends BaseAggregator<AMSeedPriDomAggregator.Context>
        implements Aggregator<AMSeedPriDomAggregator.Context> {

    private static final long serialVersionUID = 6298800516602499546L;

    private String dunsField;
    private String domField;
    private String alexaRankField;
    private String domSrcField;
    private String isPriDomField;
    private String[] srcPriorityToMrkPriDom;

    private int dunsLoc;
    private int priDomLoc;

    public static class Context extends BaseAggregator.Context {
        String duns = null;
        String priDom = null;
        Integer alexaRank = null;
        String domSrc = null;
        String isPriDom = null;
    }

    public AMSeedPriDomAggregator(Fields fieldDeclaration, String dunsField, String priDomField, String domField,
            String alexaRankField, String domSrcField, String isPriDomField, String[] srcPriorityToMrkPriDom) {
        super(fieldDeclaration);
        this.dunsField = dunsField;
        this.domField = domField;
        this.alexaRankField = alexaRankField;
        this.domSrcField = domSrcField;
        this.isPriDomField = isPriDomField;
        this.dunsLoc = namePositionMap.get(dunsField);
        this.priDomLoc = namePositionMap.get(priDomField);
        this.srcPriorityToMrkPriDom = srcPriorityToMrkPriDom;
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        Object grpObj = group.getObject(dunsField);
        if (grpObj == null) {
            return true;
        }
        if (grpObj instanceof Utf8) {
            return StringUtils.isBlank(grpObj.toString());
        }
        if (grpObj instanceof String) {
            return StringUtils.isBlank((String) grpObj);
        }
        return true;
    }

    @Override
    protected Context initializeContext(TupleEntry group) {
        return new Context();
    }

    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {
        if (StringUtils.isEmpty(context.priDom) && !StringUtils.isEmpty(arguments.getString(domField))) {
            return update(context, arguments);
        }
        int res = checkRuleSmallerInteger((Integer) arguments.getObject(alexaRankField), context.alexaRank);
        if (res > 0) {
            return update(context, arguments);
        } else if (res < 0) {
            return context;
        }
        for (String srcPriority : srcPriorityToMrkPriDom) {
            res = checkRuleExpectedString(arguments.getString(domSrcField), context.domSrc, srcPriority);
            if (res > 0) {
                return update(context, arguments);
            } else if (res < 0) {
                return context;
            }
        }
        res = checkRuleExpectedString(arguments.getString(isPriDomField), context.isPriDom, "Y");
        if (res > 0) {
            return update(context, arguments);
        } else if (res < 0) {
            return context;
        }
        return context;
    }

    private int checkRuleSmallerInteger(Integer checking, Integer checked) {
        if (checking != null && (checked == null || checking.intValue() < checked.intValue())) {
            return 1;
        } else if (checked != null && (checking == null || checked.intValue() < checking.intValue())) {
            return -1;
        } else {
            return 0;
        }
    }

    private int checkRuleExpectedString(String checking, String checked, String expectedValue) {
        if (!StringUtils.isEmpty(checking) && checking.equals(expectedValue)
                && (StringUtils.isEmpty(checked) || !checked.equals(expectedValue))) {
            return 1;
        } else if (!StringUtils.isEmpty(checked) && checked.equals(expectedValue)
                && (StringUtils.isEmpty(checking) || !checking.equals(expectedValue))) {
            return -1;
        } else {
            return 0;
        }
    }

    private Context update(Context context, TupleEntry arguments) {
        context.duns = arguments.getString(dunsField);
        context.priDom = arguments.getString(domField);
        context.alexaRank = (Integer) arguments.getObject(alexaRankField);
        context.domSrc = arguments.getString(domSrcField);
        context.isPriDom = arguments.getString(isPriDomField);
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        if (context.priDom != null) {
            Tuple result = Tuple.size(getFieldDeclaration().size());
            result.set(dunsLoc, context.duns);
            result.set(priDomLoc, context.priDom);
            return result;
        } else {
            return null;
        }
    }
}
