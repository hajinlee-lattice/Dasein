package com.latticeengines.dataflow.runtime.cascading.propdata.ams;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.dataflow.runtime.cascading.BaseFunction;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.config.ams.DomainOwnershipConfig;
import com.latticeengines.domain.exposed.dataflow.operations.OperationCode;
import com.latticeengines.domain.exposed.dataflow.operations.OperationLogUtils;
import com.latticeengines.domain.exposed.dataflow.operations.OperationMessage;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AMSeedCleanByDomOwnFunction extends BaseFunction {

    private static final long serialVersionUID = 8405618472006912324L;
    private String ownerRootDunsField;
    private String ownerDomainField;
    private String ownerReasonField = DomainOwnershipConfig.REASON_TYPE;
    private String amsRootDunsField = DomainOwnershipConfig.ROOT_DUNS;
    private String amsDunsField = DataCloudConstants.AMS_ATTR_DUNS;
    private int amsDomainIdx;
    private int alexaRankIdx;
    private int isPriDomIdx;

    public AMSeedCleanByDomOwnFunction(Fields fieldDeclaration, String ownerDomainField,
            String ownerRootDunsField) {
        super(fieldDeclaration, true);
        this.ownerRootDunsField = ownerRootDunsField;
        this.ownerDomainField = ownerDomainField;
        this.amsDomainIdx = namePositionMap.get(DataCloudConstants.AMS_ATTR_DOMAIN);
        this.alexaRankIdx = namePositionMap.get(DataCloudConstants.ATTR_ALEXA_RANK);
        this.isPriDomIdx = namePositionMap.get(DataCloudConstants.ATTR_IS_PRIMARY_DOMAIN);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple result = cleanDomain(arguments);
        functionCall.getOutputCollector().add(result);
    }

    private Tuple cleanDomain(TupleEntry arguments) {
        Tuple result = arguments.getTupleCopy();
        String amsRootDuns = arguments.getString(amsRootDunsField);
        String ownerRootDuns = arguments.getString(ownerRootDunsField);
        String amsDuns = arguments.getString(amsDunsField);
        String ownerDomain = arguments.getString(ownerDomainField);
        String ownerReason = arguments.getString(ownerReasonField);
        if (StringUtils.isNotBlank(amsDuns) //
                && StringUtils.isNotBlank(ownerDomain) //
                && StringUtils.isNotBlank(ownerRootDuns) //
                && !ownerRootDuns.equals(amsRootDuns)) {
            result.set(amsDomainIdx, null);
            result.set(alexaRankIdx, null);
            result.set(isPriDomIdx, DataCloudConstants.ATTR_VAL_N);
            String optLog = OperationLogUtils.buildLog(DataCloudConstants.TRANSFORMER_AMS_CLEANBY_DOM_OWNER,
                    OperationCode.CLEAN_DOM_BY_OWNER,
                    String.format(OperationMessage.CLEAN_NONOWNER_DOM, ownerRootDuns, ownerReason));
            result.set(logFieldIdx,
                    OperationLogUtils.appendLog(arguments.getString(OperationLogUtils.DEFAULT_FIELD_NAME), optLog));
        }
        return result;
    }

}
