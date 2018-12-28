package com.latticeengines.dataflow.runtime.cascading.cdl;

import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

@SuppressWarnings("rawtypes")
public class CalculatePercentile extends BaseOperation implements Buffer {
    private static final long serialVersionUID = -7641395420638947868L;
    private int minPct;
    private int maxPct;
    private String countFieldName;
    private int scoreFieldPos;
    private String rawScoreFieldName;

    public CalculatePercentile(Fields fieldDescription, int minPct, int maxPct, String scoreFieldName,
            String countFieldName, String rawScoreFieldName) {
        super(fieldDescription);

        this.minPct = minPct;
        this.maxPct = maxPct;
        this.countFieldName = countFieldName;
        this.rawScoreFieldName = rawScoreFieldName;
        this.scoreFieldPos = fieldDescription.getPos(scoreFieldName);

        if (scoreFieldPos == -1) {
            throw new RuntimeException("Cannot find field " + scoreFieldName + " from metadata");
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        TupleEntryCollector collector = bufferCall.getOutputCollector();
        Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();

        SimplePercentileCalculator percentileCalculator = new SimplePercentileCalculator(minPct, maxPct);

        int currentPos = 0;

        while (iter.hasNext()) {
            TupleEntry tuple = iter.next();
            long totalCount = tuple.getLong(countFieldName);
            double rawScore = tuple.getDouble(rawScoreFieldName);

            Integer pct = percentileCalculator.compute(totalCount, currentPos, rawScore);

            Tuple result = tuple.getTupleCopy();
            result.set(scoreFieldPos, pct);

            collector.add(result);
            currentPos++;
        }
    }
}
