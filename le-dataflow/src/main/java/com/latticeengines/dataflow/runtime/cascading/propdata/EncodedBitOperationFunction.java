package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.io.IOException;
import java.util.BitSet;

import com.latticeengines.common.exposed.util.BitCodecUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class EncodedBitOperationFunction extends BaseOperation implements Function {
    private static final long serialVersionUID = -5661515157418072693L;

    private String leftEncCol;
    private String rightEncCol;
    private Operation op;

    public EncodedBitOperationFunction(String newEncCol, String leftEncCol, String rightEncCol, Operation op) {
        super(new Fields(newEncCol));
        this.leftEncCol = leftEncCol;
        this.rightEncCol = rightEncCol;
        this.op = op;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String leftEncStr = arguments.getString(leftEncCol);
        String rightEncStr = arguments.getString(rightEncCol);
        if (leftEncStr == null && rightEncStr == null) {
            functionCall.getOutputCollector().add(Tuple.size(1));
            return;
        }
        if (leftEncStr == null) {
            functionCall.getOutputCollector().add(new Tuple(rightEncStr));
            return;
        }
        if (rightEncStr == null) {
            functionCall.getOutputCollector().add(new Tuple(leftEncStr));
            return;
        }
        try {
            BitSet leftEncBits = BitCodecUtils.strToBits(leftEncStr);
            BitSet rightEncBits = BitCodecUtils.strToBits(rightEncStr);
            switch (op) {
            case AND:
                leftEncBits.and(rightEncBits);
                break;
            case OR:
                leftEncBits.or(rightEncBits);
                break;
            default:
                break;
            }
            functionCall.getOutputCollector().add(new Tuple(BitCodecUtils.bitsToStr(leftEncBits)));
            return;
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Fail to finish bit operation on %s and %s", leftEncStr, rightEncStr));
        }
    }

    public enum Operation {
        AND, OR
    }
}
