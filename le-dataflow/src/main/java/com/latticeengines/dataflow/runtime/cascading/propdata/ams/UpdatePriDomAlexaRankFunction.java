package com.latticeengines.dataflow.runtime.cascading.propdata.ams;

import com.latticeengines.dataflow.runtime.cascading.BaseFunction;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.dataflow.operations.OperationCode;
import com.latticeengines.domain.exposed.dataflow.operations.OperationLogUtils;
import com.latticeengines.domain.exposed.dataflow.operations.OperationMessage;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class UpdatePriDomAlexaRankFunction extends BaseFunction {

    private static final long serialVersionUID = -462152396830896380L;

    private String secDomainField = DataCloudConstants.ORBSEC_ATTR_SECDOM;
    private String primDomainField = DataCloudConstants.ORBSEC_ATTR_PRIDOM;
    private String primDomAlexaRankField;
    private int amsDomIdx;
    private int alexaRankIdx;
    private int domSrcIdx;

    public UpdatePriDomAlexaRankFunction(Fields fieldDeclaration, String primDomAlexaRankField) {
        super(fieldDeclaration, true);
        this.primDomAlexaRankField = primDomAlexaRankField;
        amsDomIdx = namePositionMap.get(DataCloudConstants.AMS_ATTR_DOMAIN);
        alexaRankIdx = namePositionMap.get(DataCloudConstants.ATTR_ALEXA_RANK);
        domSrcIdx = namePositionMap.get(DataCloudConstants.AMS_ATTR_DOMAIN_SOURCE);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple result = updateDomain(arguments);
        functionCall.getOutputCollector().add(result);
    }

    private Tuple updateDomain(TupleEntry arguments) {
        Tuple result = arguments.getTupleCopy();
        if (arguments.getString(secDomainField) != null) {
            result.set(amsDomIdx, arguments.getString(primDomainField));
            result.set(alexaRankIdx, arguments.getObject(primDomAlexaRankField));
            result.set(domSrcIdx, DataCloudConstants.DOMSRC_ORB);
            String optLog = OperationLogUtils.buildLog(DataCloudConstants.TRANSFORMER_AMS_CLEANBY_DOM_OWNER,
                    OperationCode.SECDOM_TO_PRI,
                    String.format(OperationMessage.REPLACE_SECDOM_WITH_PRIDOM, arguments.getString(secDomainField)));
            result.set(logFieldIdx,
                    OperationLogUtils.appendLog(arguments.getString(OperationLogUtils.DEFAULT_FIELD_NAME), optLog));
        }
        return result;
    }

}
