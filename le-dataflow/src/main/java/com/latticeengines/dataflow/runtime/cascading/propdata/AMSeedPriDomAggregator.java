package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class AMSeedPriDomAggregator
        extends BaseAggregator<AMSeedPriDomAggregator.Context>
        implements Aggregator<AMSeedPriDomAggregator.Context> {

    private static final Log log = LogFactory.getLog(AMSeedPriDomAggregator.class);

    private static final long serialVersionUID = 6298800516602499546L;

    private String dunsField;
    private String domField;
    private String alexaRankField;
    private String domSrcField;
    private String isPriDomField;
    private String[] srcPriorityToMrkPriDom;
    private String duDomsField;
    private String duDunsField;

    private int dunsLoc;
    private int priDomLoc;

    public static class Context extends BaseAggregator.Context {
        String domain = null;
        String duns = null;
        String priDom = null;
        Integer alexaRank = null;
        String domSrc = null;
        String isPriDom = null;
        Set<String> duDoms = null;
    }

    public AMSeedPriDomAggregator(Fields fieldDeclaration, String dunsField, String priDomField, String domField,
            String alexaRankField, String domSrcField, String isPriDomField, String[] srcPriorityToMrkPriDom,
            String duDomsField, String duDunsField) {
        super(fieldDeclaration);
        this.dunsField = dunsField;
        this.domField = domField;
        this.alexaRankField = alexaRankField;
        this.domSrcField = domSrcField;
        this.isPriDomField = isPriDomField;
        this.dunsLoc = namePositionMap.get(dunsField);
        this.priDomLoc = namePositionMap.get(priDomField);
        this.srcPriorityToMrkPriDom = srcPriorityToMrkPriDom;
        this.duDomsField = duDomsField;
        this.duDunsField = duDunsField;
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
        if (context.duDoms == null) {
            context.duDoms = parseDuDoms(arguments.getString(duDomsField));
        }
        if (context.priDom == null && StringUtils.isNotBlank(arguments.getString(domField))) {
            return update(context, arguments);
        }
        int res = checkRuleDiffWithDuDoms(context, arguments);
        if (res > 0) {
            return update(context, arguments);
        } else if (res < 0) {
            return context;
        }
        res = checkRuleSmallerInteger((Integer) arguments.getObject(alexaRankField), context.alexaRank);
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

    private int checkRuleDiffWithDuDoms(Context context, TupleEntry arguments) {
        String duDuns = arguments.getString(duDunsField);
        String duns = arguments.getString(dunsField);
        if (StringUtils.isBlank(duDuns) || StringUtils.isBlank(duns) || duDuns.equals(duns) || context.duDoms == null) {
            return 0;
        }
        String checking = arguments.getString(domField);
        String checked = context.domain;
        if (StringUtils.isNotBlank(checking) && !context.duDoms.contains(checking)
                && (StringUtils.isBlank(checked) || context.duDoms.contains(checked))) {
            return 1;
        } else if (StringUtils.isNotBlank(checked) && !context.duDoms.contains(checked)
                && (StringUtils.isBlank(checking) || context.duDoms.contains(checking))) {
            return -1;
        } else {
            return 0;
        }
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
        if (StringUtils.isNotBlank(checking) && checking.equals(expectedValue)
                && (StringUtils.isBlank(checked) || !checked.equals(expectedValue))) {
            return 1;
        } else if (StringUtils.isNotBlank(checked) && checked.equals(expectedValue)
                && (StringUtils.isBlank(checking) || !checking.equals(expectedValue))) {
            return -1;
        } else {
            return 0;
        }
    }

    private Context update(Context context, TupleEntry arguments) {
        context.domain = arguments.getString(domField);
        context.duns = arguments.getString(dunsField);
        context.priDom = arguments.getString(domField);
        context.alexaRank = (Integer) arguments.getObject(alexaRankField);
        context.domSrc = arguments.getString(domSrcField);
        context.isPriDom = arguments.getString(isPriDomField);
        return context;
    }

    private Set<String> parseDuDoms(String duDoms) {
        if (StringUtils.isBlank(duDoms)) {
            return null;
        }
        log.info("Parsing DuDoms: " + duDoms);
        String[] duDomArr = duDoms.split("\\|\\|");
        Set<String> parsed = new HashSet<>(Arrays.asList(duDomArr));
        return parsed;
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
