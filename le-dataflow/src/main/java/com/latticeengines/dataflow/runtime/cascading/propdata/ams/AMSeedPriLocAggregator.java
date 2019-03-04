package com.latticeengines.dataflow.runtime.cascading.propdata.ams;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.common.exposed.util.LocationUtils;
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

public class AMSeedPriLocAggregator extends BaseAggregator<AMSeedPriLocAggregator.Context>
        implements Aggregator<AMSeedPriLocAggregator.Context> {

    private static final long serialVersionUID = 6246503522063890526L;

    private List<String> groupFields;
    private int idIdx;
    private OperationCode operationCode;

    public AMSeedPriLocAggregator(Fields fieldDeclaration, List<String> groupFields, String idField) {
        // Will append LE_OperationLog to track operation logs
        super(fieldDeclaration, true);
        this.groupFields = groupFields;
        idIdx = namePositionMap.get(idField);
        operationCode = getOperationCode(groupFields);
    }

    public static class Context extends BaseAggregator.Context {
        Long id = null;
        String duns = null;
        String duDuns = null;
        String guDuns = null;
        Integer employee = null;
        Long salesVol = null;
        String isPriLoc = null;
        String isPriAct = null;
        String country = null;
        String priLocReason = null;
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
        res = RuleBasedComparator.preferNonBlankString(
                arguments.getString(DataCloudConstants.AMS_ATTR_DUNS), context.duns);
        if (res > 0) {
            return update(context, arguments, OperationMessage.HAS_DUNS);
        } else if (res < 0) {
            return context;
        }
        res = RuleBasedComparator.preferNonBlankString(
                arguments.getString(DataCloudConstants.ATTR_DU_DUNS), context.duDuns);
        if (res > 0) {
            return update(context, arguments, OperationMessage.HAS_DU_DUNS);
        } else if (res < 0) {
            return context;
        }
        res = RuleBasedComparator.preferLargerLongWithThreshold(
                (Long) arguments.getObject(DataCloudConstants.ATTR_SALES_VOL_US), context.salesVol,
                100000000, 10000000);
        if (res > 0) {
            long gap = ((Long) arguments.getObject(DataCloudConstants.ATTR_SALES_VOL_US)).longValue()
                    - (context.salesVol != null ? context.salesVol.longValue() : 0L);
            String log = String.format(OperationMessage.LARGER_VAL_WITH_THRES, DataCloudConstants.ATTR_SALES_VOL_US,
                    String.valueOf((Long) arguments.getObject(DataCloudConstants.ATTR_SALES_VOL_US)), "100000000",
                    String.valueOf(gap), "10000000");
            return update(context, arguments, log);
        } else if (res < 0) {
            return context;
        }
        res = RuleBasedComparator.preferEqualStrings(
                arguments.getString(DataCloudConstants.AMS_ATTR_DUNS),
                arguments.getString(DataCloudConstants.ATTR_DU_DUNS), context.duns, context.duDuns, false, true);
        if (res > 0) {
            return update(context, arguments, OperationMessage.IS_DU_DUNS);
        } else if (res < 0) {
            return context;
        }
        res = RuleBasedComparator.preferEqualStrings(
                arguments.getString(DataCloudConstants.AMS_ATTR_DUNS),
                arguments.getString(DataCloudConstants.ATTR_GU_DUNS), context.duns, context.guDuns, false, true);
        if (res > 0) {
            return update(context, arguments, OperationMessage.IS_GU_DUNS);
        } else if (res < 0) {
            return context;
        }
        res = RuleBasedComparator.preferLargerLong(
                (Long) arguments.getObject(DataCloudConstants.ATTR_SALES_VOL_US), context.salesVol);
        if (res > 0) {
            String log = String.format(OperationMessage.LARGER_VAL, DataCloudConstants.ATTR_SALES_VOL_US,
                    String.valueOf((Long) arguments.getObject(DataCloudConstants.ATTR_SALES_VOL_US)));
            return update(context, arguments, log);
        } else if (res < 0) {
            return context;
        }
        res = RuleBasedComparator.preferBooleanValuedStringAsTrue(
                arguments.getString(DataCloudConstants.ATTR_IS_PRIMARY_LOCATION), context.isPriLoc);
        if (res > 0) {
            String log = String.format(OperationMessage.ORI_PRILOC_FLAG,
                    arguments.getString(DataCloudConstants.ATTR_IS_PRIMARY_LOCATION));
            return update(context, arguments, log);
        } else if (res < 0) {
            return context;
        }
        res = RuleBasedComparator.preferExpectedString(
                arguments.getString(DataCloudConstants.AMS_ATTR_COUNTRY), context.country,
                LocationUtils.USA, true);
        if (res > 0) {
            return update(context, arguments, OperationMessage.USA_ACT);
        } else if (res < 0) {
            return context;
        }
        res = RuleBasedComparator.preferLargerInteger(
                (Integer) arguments.getObject(DataCloudConstants.ATTR_EMPLOYEE_HERE),
                context.employee);
        if (res > 0) {
            String log = String.format(OperationMessage.LARGER_VAL, DataCloudConstants.ATTR_EMPLOYEE_HERE,
                    String.valueOf((Integer) arguments.getObject(DataCloudConstants.ATTR_EMPLOYEE_HERE)));
            return update(context, arguments, log);
        } else if (res < 0) {
            return context;
        }
        if (context.id == null) {
            return update(context, arguments, OperationMessage.RANDOM);
        }
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        if (context.id != null) {
            Tuple result = Tuple.size(getFieldDeclaration().size());
            result.set(idIdx, context.id);
            result.set(logFieldIdx, OperationLogUtils.buildLog(DataCloudConstants.TRANSFORMER_AMSEED_PRIACT_FIX,
                    operationCode, context.priLocReason));
            return result;
        } else {
            return null;
        }
    }

    private Context update(Context context, TupleEntry arguments, String priLocReason) {
        context.id = (Long) arguments.getObject(DataCloudConstants.LATTICE_ID);
        context.duns = arguments.getString(DataCloudConstants.AMS_ATTR_DUNS);
        context.duDuns = arguments.getString(DataCloudConstants.ATTR_DU_DUNS);
        context.guDuns = arguments.getString(DataCloudConstants.ATTR_GU_DUNS);
        context.employee = (Integer) arguments.getObject(DataCloudConstants.ATTR_EMPLOYEE_HERE);
        context.salesVol = (Long) arguments.getObject(DataCloudConstants.ATTR_SALES_VOL_US);
        context.isPriLoc = arguments.getString(DataCloudConstants.ATTR_IS_PRIMARY_LOCATION);
        context.isPriAct = arguments.getString(DataCloudConstants.ATTR_IS_PRIMARY_ACCOUNT);
        context.country = arguments.getString(DataCloudConstants.AMS_ATTR_COUNTRY);
        context.priLocReason = priLocReason;
        return context;
    }

    private OperationCode getOperationCode(List<String> groupFields) {
        String[] priLocGrpFields = { DataCloudConstants.AMS_ATTR_DOMAIN };
        Set<String> priLocGrpFieldSet = new HashSet<>(Arrays.asList(priLocGrpFields));
        String[] priCtryGrpFields = { DataCloudConstants.AMS_ATTR_DOMAIN, DataCloudConstants.AMS_ATTR_COUNTRY };
        Set<String> priCtryGrpFieldSet = new HashSet<>(Arrays.asList(priCtryGrpFields));
        String[] priStGrpFields = { DataCloudConstants.AMS_ATTR_DOMAIN, DataCloudConstants.AMS_ATTR_COUNTRY,
                DataCloudConstants.AMS_ATTR_STATE };
        Set<String> priStGrpFieldSet = new HashSet<>(Arrays.asList(priStGrpFields));
        String[] priZipGrpFields = { DataCloudConstants.AMS_ATTR_DOMAIN, DataCloudConstants.AMS_ATTR_COUNTRY,
                DataCloudConstants.AMS_ATTR_ZIP };
        Set<String> priZipGrpFieldSet = new HashSet<>(Arrays.asList(priZipGrpFields));
        if (groupFields.size() == priLocGrpFields.length
                && groupFields.stream().allMatch(f -> priLocGrpFieldSet.contains(f))) {
            return OperationCode.IS_PRI_LOC;
        }
        if (groupFields.size() == priCtryGrpFields.length
                && groupFields.stream().allMatch(f -> priCtryGrpFieldSet.contains(f))) {
            return OperationCode.IS_PRI_CTRY;
        }
        if (groupFields.size() == priStGrpFields.length
                && groupFields.stream().allMatch(f -> priStGrpFieldSet.contains(f))) {
            return OperationCode.IS_PRI_ST;
        }
        if (groupFields.size() == priZipGrpFields.length
                && groupFields.stream().allMatch(f -> priZipGrpFieldSet.contains(f))) {
            return OperationCode.IS_PRI_ZIP;
        }
        throw new RuntimeException("Cannot decide OperationCode");
    }
}
