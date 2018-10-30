package com.latticeengines.dataflow.runtime.cascading.propdata.ams;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;
import com.latticeengines.dataflow.runtime.cascading.propdata.util.RuleBasedComparator;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.dataflow.operations.OperationCode;
import com.latticeengines.domain.exposed.dataflow.operations.OperationLogUtils;
import com.latticeengines.domain.exposed.dataflow.operations.OperationMessage;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class AMSeedPriDomAggregator extends BaseAggregator<AMSeedPriDomAggregator.Context>
        implements Aggregator<AMSeedPriDomAggregator.Context> {

    private static final long serialVersionUID = 6298800516602499546L;

    private String dunsField;
    private String domField;
    private String alexaRankField;
    private String domSrcField;
    private String isPriDomField;
    private String[] srcPriorityToMrkPriDom;
    private String duDomsField;
    private String[] goldenDomSrcs;

    private int dunsLoc;
    private int priDomLoc;

    public AMSeedPriDomAggregator(Fields fieldDeclaration, String dunsField, String priDomField,
            String domField, String alexaRankField, String domSrcField, String isPriDomField,
            String[] srcPriorityToMrkPriDom, String duDomsField, String[] goldenDomSrcs) {
        // Will append LE_OperationLog to track operation logs
        super(fieldDeclaration, true);
        this.dunsField = dunsField;
        this.domField = domField;
        this.alexaRankField = alexaRankField;
        this.domSrcField = domSrcField;
        this.isPriDomField = isPriDomField;
        this.dunsLoc = namePositionMap.get(dunsField);
        this.priDomLoc = namePositionMap.get(priDomField);
        this.srcPriorityToMrkPriDom = srcPriorityToMrkPriDom;
        this.duDomsField = duDomsField;
        this.goldenDomSrcs = goldenDomSrcs;
    }

    public static class Context extends BaseAggregator.Context {
        String domain = null;
        String duns = null;
        String priDom = null;
        Integer alexaRank = null;
        String domSrc = null;
        String isPriDom = null;
        Set<String> duDoms = null;
        String priDomReason = null;
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
        int res;
        for (String srcPriority : goldenDomSrcs) {
            res = RuleBasedComparator.preferExpectedString(arguments.getString(domSrcField), context.domSrc,
                    srcPriority, true);
            if (res > 0) {
                return update(context, arguments,
                        String.format(OperationMessage.DOMAIN_SRC, arguments.getString(domSrcField)));
            } else if (res < 0) {
                return context;
            }
        }
        res = RuleBasedComparator.preferSmallerInteger((Integer) arguments.getObject(alexaRankField),
                context.alexaRank);
        if (res > 0) {
            return update(context, arguments,
                    String.format(OperationMessage.LOW_ALEXA_RANK, ((Integer) arguments.getObject(alexaRankField))));
        } else if (res < 0) {
            return context;
        }
        for (String srcPriority : srcPriorityToMrkPriDom) {
            res = RuleBasedComparator.preferExpectedString(arguments.getString(domSrcField), context.domSrc,
                    srcPriority, false);
            if (res > 0) {
                return update(context, arguments,
                        String.format(OperationMessage.DOMAIN_SRC, arguments.getString(domSrcField)));
            } else if (res < 0) {
                return context;
            }
        }
        res = RuleBasedComparator.preferExpectedString(arguments.getString(isPriDomField), context.isPriDom,
                DataCloudConstants.ATTR_VAL_Y, false);
        if (res > 0) {
            return update(context, arguments,
                    String.format(OperationMessage.ORI_PRIDOM_FLAG, arguments.getString(isPriDomField)));
        } else if (res < 0) {
            return context;
        }
        if (context.priDom == null && StringUtils.isNotBlank(arguments.getString(domField))) {
            return update(context, arguments, OperationMessage.RANDOM);
        }
        return context;
    }

    private Context update(Context context, TupleEntry arguments, String priDomReason) {
        context.domain = arguments.getString(domField);
        context.duns = arguments.getString(dunsField);
        context.priDom = arguments.getString(domField);
        context.alexaRank = (Integer) arguments.getObject(alexaRankField);
        context.domSrc = arguments.getString(domSrcField);
        context.isPriDom = arguments.getString(isPriDomField);
        context.priDomReason = priDomReason;
        return context;
    }

    private Set<String> parseDuDoms(String duDoms) {
        if (StringUtils.isBlank(duDoms)) {
            return null;
        }
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
            String log = OperationLogUtils.buildLog(DataCloudConstants.TRANSFORMER_AMSEED_MARKER,
                    OperationCode.IS_PRIMARY_DOMAIN, context.priDomReason);
            result.set(logFieldIdx, log);
            return result;
        } else {
            return null;
        }
    }
}
