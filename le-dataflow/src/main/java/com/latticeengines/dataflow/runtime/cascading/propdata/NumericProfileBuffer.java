package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.math3.stat.descriptive.moment.Kurtosis;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.KVDepivotStrategy;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.DiscreteBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.IntervalBucket;
import com.latticeengines.domain.exposed.util.ActivityMetricsUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class NumericProfileBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = -4591759360012525636L;

    private boolean equalSized;
    private int buckets;
    private int minBucketSize;
    private String keyAttr;
    private Map<String, Class<?>> classes;
    private Map<String, String> decStrs;
    private boolean sorted;
    private Map<String, Integer> namePositionMap;
    private double[] dVals;
    private double[] dValsRounded;

    public NumericProfileBuffer(Fields fieldDecl, String keyAttr, Map<String, Class<?>> classes,
            Map<String, String> decStrs, boolean equalSized, int buckets, int minBucketSize,
            boolean sorted) {
        super(fieldDecl);
        this.keyAttr = keyAttr;
        this.classes = classes;
        this.decStrs = decStrs;
        this.equalSized = equalSized;
        this.buckets = buckets;
        this.minBucketSize = minBucketSize;
        this.sorted = sorted;
        this.namePositionMap = getPositionMap(fieldDeclaration);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        String attr = bufferCall.getGroup().getString(keyAttr);
        Class<?> cls = classes.get(attr);
        String valAttr = KVDepivotStrategy.valueAttr(cls);

        List<Comparable> vals = new ArrayList<>();
        Iterator<TupleEntry> argIter = bufferCall.getArgumentsIterator();
        while (argIter.hasNext()) {
            TupleEntry args = argIter.next();
            Object val = args.getObject(valAttr);
            if (val == null) {
                continue;
            }
            vals.add((Comparable) val);
        }
        if (!sorted) {
            Collections.sort(vals);
        }
        doubleTransform(vals, cls);
        roundTransform();
        keepRoundedValues();
        List<Integer> boundIdxes = equalSized ? findBoundIdxesEqualSized() : findBoundIdxesByDist();
        settleResult(bufferCall, boundIdxes, cls);
    }

    private void settleResult(BufferCall bufferCall, List<Integer> boundIdxes, Class<?> cls) {
        List<Number> boundaries = new ArrayList<>();
        if (boundIdxes.size() <= 2) {
            boundaries.addAll(findDistinctValueBoundaries(cls));
        } else {
            for (int i = 1; i < boundIdxes.size() - 1; i++) {
                boundaries.add(numberTransform(dValsRounded[boundIdxes.get(i)], cls));
            }
        }
        if (boundaries.size() == 0) {
            bufferCall.getOutputCollector().add(setupRetainedAttrTuple(bufferCall));
        } else {
            if (ActivityMetricsUtils.isSpendChangeAttr(bufferCall.getGroup().getString(keyAttr))) {
                boundaries = ActivityMetricsUtils.insertZeroBndForSpendChangeBkt(boundaries);
            }
            bufferCall.getOutputCollector().add(setupIntervalBucketTuple(bufferCall, boundaries));
        }
    }

    private Tuple setupIntervalBucketTuple(BufferCall bufferCall, List<Number> boundaries) {
        IntervalBucket bucket = new IntervalBucket();
        bucket.setBoundaries(boundaries);

        Tuple result = Tuple.size(getFieldDeclaration().size());
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_ATTRNAME),
                bufferCall.getGroup().getObject(keyAttr));
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_SRCATTR),
                bufferCall.getGroup().getObject(keyAttr));
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_DECSTRAT),
                decStrs.get(bufferCall.getGroup().getString(keyAttr)));
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_ENCATTR), null);
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_LOWESTBIT), null);
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_NUMBITS), null);
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_BKTALGO),
                JsonUtils.serialize(bucket));
        return result;
    }

    private Tuple setupRetainedAttrTuple(BufferCall bufferCall) {
        Tuple result = Tuple.size(getFieldDeclaration().size());
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_ATTRNAME),
                bufferCall.getGroup().getObject(keyAttr));
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_SRCATTR),
                bufferCall.getGroup().getObject(keyAttr));
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_DECSTRAT),
                decStrs.get(bufferCall.getGroup().getString(keyAttr)));
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_ENCATTR), null);
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_LOWESTBIT), null);
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_NUMBITS), null);
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_BKTALGO), //
                JsonUtils.serialize(new DiscreteBucket()));
        return result;
    }

    private List<Number> findDistinctValueBoundaries(Class<?> cls) {
        Set<Double> distinctValues = new HashSet<>();
        for (double dVal : dValsRounded) {
            if (!distinctValues.contains(dVal)) {
                distinctValues.add(dVal);
            }
        }
        List<Double> sorted = new ArrayList<>(distinctValues);
        Collections.sort(sorted);
        if (sorted.size() > 2) {
            sorted.remove(0);
            sorted.remove(sorted.size() - 1);
        }
        Random r = new Random();
        int size = sorted.size();
        for (int i = 0; i < size - (buckets - 1); i++) {
            sorted.remove(r.nextInt(sorted.size()));
        }
        List<Number> boundaries = new ArrayList<>();
        for (Double dVal : sorted) {
            boundaries.add(numberTransform(dVal, cls));
        }
        return boundaries;
    }

    /**
     * Split into roughly equally sized buckets Genertes a list of small bins as
     * a first step in bucketing. Bins are roughly equally-sized, built from a
     * sorted list. If a possible boundary straddles the same value, make sure
     * the same value either belongs to the left side bin, or to the right side
     * bin, or forms a bin by itself. Returns: a list of index positions at
     * boundaries of these bins
     */
    private List<Integer> findBoundIdxesEqualSized() {
        // DVal dVal = attrDVals.get(attr);
        // double[] vals = dVal.getVals();
        List<Integer> buckIdxes = new ArrayList<>();
        int divider = 3;
        int stepSize = dVals.length / (buckets * divider);
        int numPtsInBin = stepSize * divider;
        // if there is not enough data, no bucketing is needed
        if (stepSize == 0) {
            buckIdxes.add(0);
            buckIdxes.add(dVals.length);
            return buckIdxes;
        }
        // all other cases - make bins by step size
        buckIdxes.add(0);
        int posCurr = 0;
        while (posCurr < dVals.length) {
            // move the position by stepSize
            posCurr = posCurr + stepSize;
            // end iteration when reaching the end
            if (posCurr >= dVals.length) {
                buckIdxes.add(dVals.length);
                break;
            }
            // if the value at current position is the same as the starting
            // value of the current bucket,
            // or there are still not sufficient points to make a new bucket,
            // or if the remaining buckets are not sufficient to make a new
            // bucket,
            // keep expanding current bucket
            else if (dVals[posCurr] == dVals[buckIdxes.get(buckIdxes.size() - 1)]
                    || posCurr - buckIdxes.get(buckIdxes.size() - 1) < numPtsInBin
                    || dVals.length - posCurr < numPtsInBin) {
                continue;
            }
            // when a new bucket is possible, check the new boundary point
            // if values at each side of the boundary point are different
            // create a new bucket
            else if (dVals[posCurr] > dVals[posCurr - 1]) {
                buckIdxes.add(posCurr);
            }
            // potential boundary separates the same value into two bins
            // need to move it
            else {
                // find position before first appearance of boundary value
                int idxL = findWhereValueChanges(posCurr, -1);
                // if there are enough points without boundary values to make a
                // bucket,
                // back up to the left of first occurence of boundary values
                // make a new bucket and start from there
                if (idxL + 1 - buckIdxes.get(buckIdxes.size() - 1) >= numPtsInBin) {
                    posCurr = idxL + 1;
                    buckIdxes.add(posCurr);
                }
                // otherwise, merge all boundary value occrences into left
                // bucket
                else {
                    posCurr = findWhereValueChanges(posCurr, 1);
                    buckIdxes.add(posCurr);
                }
            }
        }

        return buckIdxes;
    }

    /**
     * find where the next value is different idx: idx to start searching
     * direction = 1 searches to the right direction = -1 searches to the left
     * returns: index of the next encountered element with different value from
     * xArr[idx]
     */
    private int findWhereValueChanges(int idx, int direction) {
        while (idx + direction > 0 && idx + direction < dVals.length) {
            if (dVals[idx + direction] == dVals[idx]) {
                idx = idx + direction;
            } else {
                break;
            }
        }
        // return the index of first occurence of a different x value
        // if already at the end, return the end point
        return Math.min(Math.max(idx + direction, 0), dVals.length - 1);
    }

    /**
     * genertes a list of small bins as a first step in bucketing. bins are
     * built from a sorted list. returns: a list of index positions at
     * boundaries of these bins
     */
    private List<Integer> findBoundIdxesByDist() {
        List<Integer> buckIdxes = new ArrayList<>();
        // if there is not enough data, no bucketing is needed
        if (dVals.length <= minBucketSize) {
            buckIdxes.add(0);
            buckIdxes.add(dVals.length);
            return buckIdxes;
        }
        // all other cases - make bins by range
        if (isGeoDist()) {
            logTransform();
        }
        double xMin = dVals[0];
        double xMax = dVals[dVals.length - 1];
        double[] boundaries = new double[buckets - 1];
        for (int i = 1; i < buckets; i++) {
            boundaries[i - 1] = xMin + (xMax - xMin) * i / buckets;
        }
        buckIdxes.add(0);
        for (double b : boundaries) {
            int bIdx = bisectLeft(b);
            if ((bIdx - buckIdxes.get(buckIdxes.size() - 1)) >= minBucketSize) {
                buckIdxes.add(bIdx);
            }
        }
        if (xMax > buckIdxes.get(buckIdxes.size() - 1) - minBucketSize) {
            buckIdxes.add(dVals.length);
        } else {
            buckIdxes.set(buckIdxes.size() - 1, dVals.length);
        }
        return buckIdxes;
    }

    private int bisectLeft(double key) {
        int bIdx = Arrays.binarySearch(dVals, key);
        if (bIdx >= 0) {
            while (bIdx > 0 && dVals[bIdx] == dVals[bIdx - 1]) {
                bIdx--;
            }
        } else {
            bIdx = -(bIdx + 1);
        }
        return bIdx;
    }

    private void keepRoundedValues() {
        for (int i = 0; i < dVals.length; i++) {
            dValsRounded[i] = dVals[i];
        }
    }

    private void doubleTransform(List<Comparable> vals, Class<?> cls) {
        dVals = new double[vals.size()];
        dValsRounded = new double[vals.size()];
        if (cls.equals(Integer.class)) {
            for (int i = 0; i < vals.size(); i++) {
                dVals[i] = ((Integer) vals.get(i)).doubleValue();
            }
        } else if (cls.equals(Long.class)) {
            for (int i = 0; i < vals.size(); i++) {
                dVals[i] = ((Long) vals.get(i)).doubleValue();
            }
        } else if (cls.equals(Float.class)) {
            for (int i = 0; i < vals.size(); i++) {
                dVals[i] = ((Float) vals.get(i)).doubleValue();
            }
        } else if (cls.equals(Double.class)) {
            for (int i = 0; i < vals.size(); i++) {
                dVals[i] = ((Double) vals.get(i)).doubleValue();
            }
        } else {
            throw new UnsupportedOperationException(String
                    .format("type %s is not supported in numeric profiling", cls.getSimpleName()));
        }

    }

    private void logTransform() {
        for (int i = 0; i < dVals.length; i++) {
            if (dVals[i] > 0) {
                dVals[i] = Math.log10(dVals[i]);
            } else if (dVals[i] < 0) {
                dVals[i] = -Math.log10(-dVals[i]);
            }
        }
    }

    private void roundTransform() {
        for (int i = 0; i < dVals.length; i++) {
            dVals[i] = roundTo5(dVals[i]);
        }
    }

    private Number numberTransform(double x, Class<?> cls) {
        if (cls.equals(Integer.class)) {
            return (Number) Integer.valueOf((int) x);
        } else if (cls.equals(Long.class)) {
            return (Number) Long.valueOf((long) x);
        } else if (cls.equals(Float.class)) {
            return (Number) Float.valueOf((float) x);
        } else if (cls.equals(Double.class)) {
            return (Number) Double.valueOf(x);
        } else {
            throw new UnsupportedOperationException(String.format(
                    "type %s type is not supported in numeric profiling", cls.getSimpleName()));
        }
    }

    private boolean isGeoDist() {
        Kurtosis k = new Kurtosis();
        return k.evaluate(dVals) >= 3.0;
    }

    private double formatDouble(double x, int digits) {
        double base = Math.pow(10, (double) digits);
        return (BigDecimal.valueOf(Math.round(x * base)).divide(BigDecimal.valueOf(base)))
                .doubleValue();
    }

    private double roundTo(double x, int sigDigits) {
        if (x == 0) {
            return 0;
        } else {
            int digits = sigDigits - 1 - ((int) Math.floor(Math.log10(Math.abs(x))));
            return formatDouble(x, digits);
        }
    }

    private double roundTo5(double x) {
        if (Math.abs(x) <= 10) {
            return roundTo(x, 1);
        }
        String xStr = String.valueOf((int) roundTo(x, 2));
        int secondDigit = x > 0 ? 1 : 2;
        Character[] ch1 = { '1', '2', '8', '9' };
        Set<Character> chSet1 = new HashSet<>(Arrays.asList(ch1));
        Character[] ch2 = { '3', '4', '6', '7' };
        Set<Character> chSet2 = new HashSet<>(Arrays.asList(ch2));
        if (chSet1.contains(xStr.charAt(secondDigit))) {
            return roundTo(x, 1);
        } else if (chSet2.contains(xStr.charAt(secondDigit))) {
            xStr = xStr.substring(0, secondDigit) + "5" + xStr.substring(secondDigit + 1);
        }
        return Double.valueOf(xStr);
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
