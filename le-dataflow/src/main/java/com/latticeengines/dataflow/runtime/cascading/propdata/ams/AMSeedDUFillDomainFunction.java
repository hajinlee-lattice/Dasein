package com.latticeengines.dataflow.runtime.cascading.propdata.ams;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.dataflow.runtime.cascading.BaseFunction;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.dataflow.operations.OperationLogUtils;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class AMSeedDUFillDomainFunction extends BaseFunction {

    private static final long serialVersionUID = -8373443644101345522L;

    private static final String AMS_DOMAIN = DataCloudConstants.AMS_ATTR_DOMAIN;
    private static final String DU_PRI_DOMAIN = AMSeedDUPriDomBuffer.DU_PRIMARY_DOMAIN;
    private static final String OPT_LOG = OperationLogUtils.DEFAULT_FIELD_NAME;
    private String duLogField;

    private int domainIdx;
    private int isPriDomIdx;

    public AMSeedDUFillDomainFunction(Fields fieldDeclaration, String duLogField) {
        super(fieldDeclaration, true);
        this.duLogField = duLogField;
        domainIdx = this.namePositionMap.get(DataCloudConstants.AMS_ATTR_DOMAIN);
        isPriDomIdx = this.namePositionMap.get(DataCloudConstants.ATTR_IS_PRIMARY_DOMAIN);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple result = arguments.getTupleCopy();
        // DU_PRI_DOMAIN is guaranteed to be not empty string
        if (StringUtils.isBlank(arguments.getString(AMS_DOMAIN)) && arguments.getString(DU_PRI_DOMAIN) != null) {
            result.set(domainIdx, arguments.getString(DU_PRI_DOMAIN));
            result.set(isPriDomIdx, DataCloudConstants.ATTR_VAL_Y);
            result.set(logFieldIdx,
                    OperationLogUtils.appendLog(arguments.getString(OPT_LOG), arguments.getString(duLogField)));
        }
        functionCall.getOutputCollector().add(result);
    }

}
