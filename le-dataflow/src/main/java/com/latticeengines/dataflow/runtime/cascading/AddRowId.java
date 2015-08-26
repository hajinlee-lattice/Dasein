package com.latticeengines.dataflow.runtime.cascading;

import java.beans.ConstructorProperties;

import org.apache.hadoop.mapred.Counters.Counter;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.tez.Hadoop2TezFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

@SuppressWarnings("rawtypes")
public class AddRowId extends BaseOperation implements Function {

    private static final long serialVersionUID = -266552390572921248L;
    
    private final String table;

    @ConstructorProperties({ "fieldDeclaration" })
    public AddRowId(Fields fieldDeclaration, String table) {
        super(0, fieldDeclaration);
        this.table = table;
    }
    
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        Counter counter = null;
        if (flowProcess instanceof HadoopFlowProcess) {
            counter = ((HadoopFlowProcess) flowProcess).getReporter().getCounter("LATTICE", table);
        } else if (flowProcess instanceof Hadoop2TezFlowProcess) {
            counter = ((Hadoop2TezFlowProcess) flowProcess).getReporter().getCounter("LATTICE", table);
        }
        counter.increment(1L);
        long value = counter.getValue();
        functionCall.getOutputCollector().add(new Tuple(value));
    }

}
