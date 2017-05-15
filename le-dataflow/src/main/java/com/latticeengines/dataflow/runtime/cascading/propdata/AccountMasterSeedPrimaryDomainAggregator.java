package com.latticeengines.dataflow.runtime.cascading.propdata;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class AccountMasterSeedPrimaryDomainAggregator
        extends BaseAggregator<AccountMasterSeedPrimaryDomainAggregator.Context>
        implements Aggregator<AccountMasterSeedPrimaryDomainAggregator.Context> {

    private static final long serialVersionUID = 6298800516602499546L;

    private String dunsField;
    private String domainField;
    private String alexaRankField;
    private String domainSourceField;
    private String isPrimaryDomainField;

    private int dunsLoc;
    private int primaryDomainLoc;

    public static class Context extends BaseAggregator.Context {
        String duns = null;
        String primaryDomain = null;
        Integer alexaRank = null;
        String domainSource = null;
        String isPrimaryDomain = null;
    }

    public AccountMasterSeedPrimaryDomainAggregator(Fields fieldDeclaration, String dunsField,
            String primaryDomainField, String domainField, String alexaRankField, String domainSourceField,
            String isPrimaryDomainField) {
        super(fieldDeclaration);
        this.dunsField = dunsField;
        this.domainField = domainField;
        this.alexaRankField = alexaRankField;
        this.domainSourceField = domainSourceField;
        this.isPrimaryDomainField = isPrimaryDomainField;
        this.dunsLoc = namePositionMap.get(dunsField);
        this.primaryDomainLoc = namePositionMap.get(primaryDomainField);
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
        if (StringUtils.isEmpty(context.primaryDomain) && !StringUtils.isEmpty(arguments.getString(domainField))) {
            return update(context, arguments);
        }
        int res = checkRuleSmallerInteger((Integer) arguments.getObject(alexaRankField), context.alexaRank);
        if (res > 0) {
            return update(context, arguments);
        } else if (res < 0) {
            return context;
        }
        res = checkRuleExpectedString(arguments.getString(domainSourceField), context.domainSource,
                AMSeedMergeWithDunsBuffer.LE);
        if (res > 0) {
            return update(context, arguments);
        } else if (res < 0) {
            return context;
        }
        res = checkRuleExpectedString(arguments.getString(isPrimaryDomainField), context.isPrimaryDomain, "Y");
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
        context.primaryDomain = arguments.getString(domainField);
        context.alexaRank = (Integer) arguments.getObject(alexaRankField);
        context.domainSource = arguments.getString(domainSourceField);
        context.isPrimaryDomain = arguments.getString(isPrimaryDomainField);
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        if (context.primaryDomain != null) {
            Tuple result = Tuple.size(getFieldDeclaration().size());
            result.set(dunsLoc, context.duns);
            result.set(primaryDomainLoc, context.primaryDomain);
            return result;
        } else {
            return null;
        }
    }
}
