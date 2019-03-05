package com.latticeengines.dataflow.runtime.cascading.propdata.patch;

import java.util.Arrays;
import java.util.Map;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Copy the first non-blank Domain + DUNS pair (only one field need to be non-blank to get selected) to given output
 * fields. Output Domain/DUNS field must exist in input.
 */
@SuppressWarnings("rawtypes")
public class DomainDunsSelectFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 8802589814759711914L;

    private int outputDomainIdx;
    private int outputDunsIdx;
    private int[] domainIdxs;
    private int[] dunsIdxs;

    public DomainDunsSelectFunction(
            @NotNull Fields fieldDeclaration, @NotNull String outputDomainField, @NotNull String outputDunsField,
            @NotNull String[] domainFields, @NotNull String[] dunsFields) {
        super(fieldDeclaration);
        Preconditions.checkNotNull(fieldDeclaration);
        check(outputDomainField, outputDunsField);
        check(domainFields, dunsFields);

        // retrieve field index for fast retrieval
        Map<String, Integer> posMap = getPositionMap(fieldDeclaration);
        outputDomainIdx = posMap.get(outputDomainField);
        outputDunsIdx = posMap.get(outputDunsField);
        domainIdxs = Arrays.stream(domainFields).mapToInt(posMap::get).toArray();
        dunsIdxs = Arrays.stream(dunsFields).mapToInt(posMap::get).toArray();
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        OptionalInt selectedIdx = IntStream.range(0, domainIdxs.length).filter(idx -> {
            String domain = (String) arguments.getObject(domainIdxs[idx]);
            String duns = (String) arguments.getObject(dunsIdxs[idx]);
            // one of them not blank
            return StringUtils.isNotBlank(domain) || StringUtils.isNotBlank(duns);
        }).findFirst();

        if (selectedIdx.isPresent()) {
            // copy first non-blank Domain + DUNS pair
            TupleEntry res = new TupleEntry(arguments);
            int idx = selectedIdx.getAsInt();
            String domain = (String) arguments.getObject(domainIdxs[idx]);
            String duns = (String) arguments.getObject(dunsIdxs[idx]);
            res.setRaw(outputDomainIdx, domain);
            res.setRaw(outputDunsIdx, duns);
            functionCall.getOutputCollector().add(res);
        } else {
            // all Domain + DUNS pairs are blank, output the original input
            functionCall.getOutputCollector().add(arguments);
        }
    }

    /*
     * retrieve field to index mapping
     */
    private Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        return IntStream.range(0, fieldDeclaration.size())
                .mapToObj(idx -> Pair.of((String) fieldDeclaration.get(idx), idx))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    private void check(@NotNull String outputDomainField, @NotNull String outputDunsField) {
        Preconditions.checkNotNull(outputDomainField);
        Preconditions.checkNotNull(outputDunsField);
    }

    /*
     * non-null Domain + DUNS fields for selection
     */
    private void check(@NotNull String[] domainFields, @NotNull String[] dunsFields) {
        Preconditions.checkNotNull(domainFields);
        Preconditions.checkNotNull(dunsFields);
        // same size
        Preconditions.checkArgument(domainFields.length == dunsFields.length);
        IntStream.range(0, domainFields.length).forEach(idx -> {
            Preconditions.checkNotNull(domainFields[idx]);
            Preconditions.checkNotNull(dunsFields[idx]);
        });
    }
}
