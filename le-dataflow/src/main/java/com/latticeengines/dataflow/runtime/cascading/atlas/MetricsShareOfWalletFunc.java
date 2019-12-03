package com.latticeengines.dataflow.runtime.cascading.atlas;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * % Margin on product A purchase by a given account = Round (100* ((Revenue -
 * Cost) of product A sell within last X weeks / cost of product A.))
 *
 */
@SuppressWarnings("rawtypes")
public class MetricsShareOfWalletFunc extends BaseOperation implements Function {

    private static final long serialVersionUID = -6717920362621336533L;

    private static final Logger log = LoggerFactory.getLogger(MetricsShareOfWalletFunc.class);

    private String apField;
    private String atField;
    private String spField;
    private String stField;

    public MetricsShareOfWalletFunc(String targetField, String apField, String atField,
            String spField, String stField) {
        super(new Fields(targetField));
        this.apField = apField;
        this.atField = atField;
        this.spField = spField;
        this.stField = stField;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        // treat null as 0
        double apSpend = arguments.getDouble(apField);
        double atSpend = arguments.getDouble(atField);
        double spSpend = arguments.getDouble(spField);
        double stSpend = arguments.getDouble(stField);
        if (apSpend != 0 && (atSpend == 0 || spSpend == 0 || stSpend == 0)) {
            log.error(
                    "FATAL: account spend on one product is not 0, while account total spend is 0, or segment spend on one product is 0, or segment total spend is 0");
            functionCall.getOutputCollector().add(Tuple.size(1));
            return;
        }
        if (spSpend != 0 && stSpend == 0) {
            log.error(
                    "FATAL: segment spend on one product is not 0, while segment total spend is 0");
            functionCall.getOutputCollector().add(Tuple.size(1));
            return;
        }
        if (atSpend == 0 || spSpend == 0 || stSpend == 0) {
            functionCall.getOutputCollector().add(Tuple.size(1));
            return;
        }
        double apSpendRatio = apSpend / atSpend;
        double spSpendRatio = spSpend / stSpend;
        int shareOfWallet = (int) Math.round(100 * apSpendRatio / spSpendRatio);
        functionCall.getOutputCollector().add(new Tuple(shareOfWallet));
    }
}
