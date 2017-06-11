package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.Random;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

@SuppressWarnings("rawtypes")
public class AddRandomIntFunction extends BaseOperation<AddRandomIntFunction.Context>
        implements Function<AddRandomIntFunction.Context> {

    private static final long serialVersionUID = -4369341989896797069L;

    public static class Context {
    }

    private int min;
    private int max;
    private Long seed;
    private Random rand;

    // Only work for generating a random int in the range of [min, max]
    public AddRandomIntFunction(String randomAttr, int min, int max, Long seed) {
        super(new Fields(randomAttr));
        this.min = min;
        this.max = max;
        this.seed = seed;
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<Context> operationCall) {
        if (seed == null) {
            rand = new Random(flowProcess.getCurrentSliceNum() * 10 + System.currentTimeMillis());
        } else {
            rand = new Random(flowProcess.getCurrentSliceNum() + seed);
        }
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        int r = rand.nextInt((max - min) + 1) + min;
        functionCall.getOutputCollector().add(new Tuple(r));
    }
}
