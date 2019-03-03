package com.latticeengines.dataflow.runtime.cascading.propdata.ams;

import java.util.List;

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

public class AMSeedPriDomainAggregator extends BaseAggregator<AMSeedPriDomainAggregator.Context>
        implements Aggregator<AMSeedPriDomainAggregator.Context> {
    private static final long serialVersionUID = -5799582067670721843L;
    private List<String> groupFields;
    private int idIdx;

    public AMSeedPriDomainAggregator(Fields fieldDeclaration, List<String> groupFields, String idField) {
        // Will append LE_OperationLog to track operation logs
        super(fieldDeclaration, true);
        this.groupFields = groupFields;
        idIdx = namePositionMap.get(idField);
    }

    public static class Context extends BaseAggregator.Context {
        Long id = null;
        String isPriDom = null;
        String isPriAct = null;
        String priDomReason = null;
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        for (String groupField : groupFields) {
            Object grpObj = group.getObject(groupField);
            if (grpObj == null) {
                return true;
            }
            if (grpObj instanceof Utf8 && StringUtils.isBlank(grpObj.toString())) {
                return true;
            }
            if (grpObj instanceof String && StringUtils.isBlank((String) grpObj)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected Context initializeContext(TupleEntry group) {
        return new Context();
    }

    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {
        int res = RuleBasedComparator.preferBooleanValuedStringAsTrue(
                arguments.getString(DataCloudConstants.ATTR_IS_PRIMARY_ACCOUNT), context.isPriAct);
        if (res > 0) {
            return update(context, arguments, OperationMessage.PRIMARY_ACCOUNT);
        } else if (res < 0) {
            return context;
        }
        res = RuleBasedComparator.preferBooleanValuedStringAsTrue(
                arguments.getString(DataCloudConstants.ATTR_IS_PRIMARY_DOMAIN), context.isPriDom);
        if (res > 0) {
            return update(context, arguments, null);
        } else if (res < 0) {
            return context;
        }
        if (context.id == null) {
            return update(context, arguments, null);
        }
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        if (context.id != null) {
            Tuple result = Tuple.size(getFieldDeclaration().size());
            result.set(idIdx, context.id);
            if (context.priDomReason != null) {
                result.set(logFieldIdx, OperationLogUtils.buildLog(DataCloudConstants.TRANSFORMER_AMSEED_PRIACT_FIX,
                        OperationCode.IS_PRI_DOM, context.priDomReason));
            }
            return result;
        } else {
            return null;
        }
    }

    private Context update(Context context, TupleEntry arguments, String priDomReason) {
        context.id = (Long) arguments.getObject(DataCloudConstants.LATTICE_ID);
        context.isPriDom = arguments.getString(DataCloudConstants.ATTR_IS_PRIMARY_DOMAIN);
        context.isPriAct = arguments.getString(DataCloudConstants.ATTR_IS_PRIMARY_ACCOUNT);
        context.priDomReason = priDomReason;
        return context;
    }
}
