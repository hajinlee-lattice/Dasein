package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Insight: % spend change is calculated by comparing average spend for a given
 * product of a given account in the specified time window with that of the
 * range in prior time window.
 * If lastAvgSpend is not available, spend change is -100%
 * If previousAvgSpend is not available, spend  change is 100%
 */
@SuppressWarnings("rawtypes")
public class MetricsSpendChangeFunc extends BaseOperation implements Function {

    private static final long serialVersionUID = -8810559531494520419L;

    private Map<String, Integer> namePositionMap;
    private int spendChangeLoc;
    private String lastAvgSpendField;
    private String priorAvgSpendField;
    private List<String> lastGroupByFields;
    private List<String> priorGroupByFields;
    private List<String> groupByFields;

    public MetricsSpendChangeFunc(Fields fieldDeclaration, String spendChangeField, String lastAvgSpendField,
            String priorAvgSpendField, List<String> lastGroupByFields, List<String> priorGroupByFields,
            List<String> groupByFields) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
        this.spendChangeLoc = namePositionMap.get(spendChangeField);
        this.lastAvgSpendField = lastAvgSpendField;
        this.priorAvgSpendField = priorAvgSpendField;
        this.lastAvgSpendField = lastAvgSpendField;
        this.lastGroupByFields = lastGroupByFields;
        this.priorGroupByFields = priorGroupByFields;
        this.groupByFields = groupByFields;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        double lastAvgSpend = arguments.getDouble(lastAvgSpendField); // treat null as 0
        double priorAvgSpend = arguments.getDouble(priorAvgSpendField); // treat null as 0
        int spendChange;
        if (lastAvgSpend == 0 && priorAvgSpend == 0) {
            spendChange = 0;
        } else if (priorAvgSpend != 0) {
            spendChange = (int) Math.round(100 * (lastAvgSpend - priorAvgSpend) / priorAvgSpend);
        } else {
            spendChange = 100;
        }
        Tuple result = Tuple.size(getFieldDeclaration().size());
        for (int i = 0; i < groupByFields.size(); i++) {
            String groupByVal = arguments.getString(lastGroupByFields.get(i)) != null
                    ? arguments.getString(lastGroupByFields.get(i))
                    : arguments.getString(priorGroupByFields.get(i));
            result.set(namePositionMap.get(groupByFields.get(i)), groupByVal);
        }
        result.set(spendChangeLoc, spendChange);
        functionCall.getOutputCollector().add(result);
    }

    private Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field : fieldDeclaration) {
            String fieldName = (String) field;
            positionMap.put(fieldName, pos++);
        }
        return positionMap;
    }
}
