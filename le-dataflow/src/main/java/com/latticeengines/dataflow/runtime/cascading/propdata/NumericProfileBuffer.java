package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.math3.stat.descriptive.moment.Kurtosis;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import edu.emory.mathcs.backport.java.util.Collections;

@SuppressWarnings("rawtypes")
public class NumericProfileBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = -4591759360012525636L;

    private String attr;
    private boolean equalSized;
    private int buckets;
    private int minBucketSize;
    private Class<Comparable> cls;
    private boolean sorted;
    private double[] xArr;
    private double[] xArrOri;

    public NumericProfileBuffer(Fields fieldDecl, String attr, boolean equalSized, int buckets, int minBucketSize,
            Class<Comparable> cls, boolean sorted) {
        super(fieldDecl);
        this.attr = attr;
        this.equalSized = equalSized;
        this.buckets = buckets;
        this.minBucketSize = minBucketSize;
        this.cls = cls;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        List<Object> xList = new ArrayList<>();
        Iterator<TupleEntry> argIter = bufferCall.getArgumentsIterator();
        while (argIter.hasNext()) {
            TupleEntry args = argIter.next();
            Object val = args.getObject(attr);
            if (val == null) {
                continue;
            }
            xList.add(val);
        }
        if (!sorted) {
            Collections.sort(xList);
        }
        doubleTransform(xList);
        roundTransform();
        keepOriArr();
        List<Integer> boundIdxes = equalSized ? findBoundIdxesEqualSized() : findBoundIdxesByDist();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < boundIdxes.size() - 1; i++) {
            sb.append(String.valueOf(xArrOri[boundIdxes.get(i)]) + "||");
        }
        Tuple result = Tuple.size(getFieldDeclaration().size());
        result.set(0, attr);
        result.set(1, sb.toString());
        bufferCall.getOutputCollector().add(result);
    }

    /**
     * Split into roughly equally sized buckets 
     * Genertes a list of small bins as a first step in bucketing. 
     * Bins are roughly equally-sized, built from a sorted list. 
     * If a possible boundary straddles the same value, make sure the same value either belongs to the left side bin, or to the right side bin, or forms a bin by itself. 
     * Returns: a list of index positions at boundaries of these bins
     */
    private List<Integer> findBoundIdxesEqualSized() {
        List<Integer> buckIdxes = new ArrayList<>();
        int divider = 3;
        int stepSize = xArr.length / (buckets * divider);
        int numPtsInBin = stepSize * divider;
        // if there is not enough data, no bucketing is needed
        if (stepSize == 0) {
            buckIdxes.add(0);
            buckIdxes.add(xArr.length);
            return buckIdxes;
        }
        // all other cases - make bins by step size
        buckIdxes.add(0);
        int posCurr = 0;
        while (posCurr < xArr.length) {
            // move the position by stepSize
            posCurr = posCurr + stepSize;     
            // end iteration when reaching the end
            if (posCurr >= xArr.length) {
                buckIdxes.add(xArr.length);
                break;
            }
            // if the value at current position is the same as the starting value of the current bucket, 
            // or there are still not sufficient points to make a new bucket, 
            // or if the remaining buckets are not sufficient to make a new bucket, 
            // keep expanding current bucket
            else if (xArr[posCurr] == xArr[buckIdxes.get(buckIdxes.size() - 1)]
                    || posCurr - buckIdxes.get(buckIdxes.size() - 1) < numPtsInBin
                    || xArr.length - posCurr < numPtsInBin) {
                continue;
            }
            // when a new bucket is possible, check the new boundary point
            // if values at each side of the boundary point are different
            // create a new bucket
            else if (xArr[posCurr] > xArr[posCurr - 1]) {
                buckIdxes.add(posCurr);
            }
            // potential boundary separates the same value into two bins
            // need to move it
            else {
                // find position before first appearance of boundary value
                int idxL = findWhereValueChanges(posCurr, -1);
                // if there are enough points without boundary values to make a bucket, 
                // back up to the left of first occurence of boundary values
                // make a new bucket and start from there
                if (idxL + 1 - buckIdxes.get(buckIdxes.size() - 1) >= numPtsInBin) {
                    posCurr = idxL + 1;
                    buckIdxes.add(posCurr);
                } 
                // otherwise, merge all boundary value occrences into left bucket
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
     * returns: index of the next encountered element with different value from xArr[idx]
     */
    private int findWhereValueChanges(int idx, int direction) {
        while (idx + direction > 0 && idx + direction < xArr.length) {
            if (xArr[idx + direction] == xArr[idx]) {
                idx = idx + direction;
            }
            else {
                break;
            }
        }
        // return the index of first occurence of a different x value
        // if already at the end, return the end point
        return Math.min(Math.max(idx + direction, 0), xArr.length - 1);
    }

    /**
     *genertes a list of small bins as a first step in bucketing.
     *bins are built from a sorted list.
     *returns: a list of index positions at boundaries of these bins
     */
    private List<Integer> findBoundIdxesByDist() {
        List<Integer> buckIdxes = new ArrayList<>();
        // if there is not enough data, no bucketing is needed
        if (xArr.length <= minBucketSize) {
            buckIdxes.add(0);
            buckIdxes.add(xArr.length);
            return buckIdxes;
        }
        // all other cases - make bins by range
        if (isGeoDist()) {
            logTransform();
        }
        double xMin = xArr[0];
        double xMax = xArr[xArr.length - 1];
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
            buckIdxes.add(xArr.length);
        }
        else {
            buckIdxes.set(buckIdxes.size() - 1, xArr.length);
        }
        return buckIdxes;
    }

    private int bisectLeft(double key) {
        int bIdx = Arrays.binarySearch(xArr, key);
        if (bIdx >= 0) {
            while (bIdx > 0 && xArr[bIdx] == xArr[bIdx - 1]) {
                bIdx--;
            }
        } else {
            bIdx = -(bIdx + 1);
        }
        return bIdx;
    }

    private void keepOriArr() {
        xArrOri = new double[xArr.length];
        for (int i = 0; i < xArrOri.length; i++) {
            xArrOri[i] = xArr[i];
        }
    }

    private void doubleTransform(List<Object> xList) {
        xArr = new double[xList.size()];
        if (cls.equals(Integer.class)) {
            for (int i = 0; i < xList.size(); i++) {
                xArr[i] = ((Integer) xList.get(i)).doubleValue();
            }
        } else if (cls.equals(Long.class)) {
            for (int i = 0; i < xList.size(); i++) {
                xArr[i] = ((Long) xList.get(i)).doubleValue();
            }
        } else if (cls.equals(Float.class)) {
            for (int i = 0; i < xList.size(); i++) {
                xArr[i] = ((Float) xList.get(i)).doubleValue();
            }
        } else if (cls.equals(Double.class)) {
            for (int i = 0; i < xList.size(); i++) {
                xArr[i] = ((Double) xList.get(i)).doubleValue();
            }
        } else {
            throw new UnsupportedOperationException(
                    String.format("%s type is not supported in numeric profiling", attr));
        }
    }

    private void logTransform() {
        for (int i = 0; i < xArr.length; i++) {
            if (xArr[i] > 0) {
                xArr[i] = Math.log10(xArr[i]);
            } else if (xArr[i] < 0) {
                xArr[i] = -Math.log10(-xArr[i]);
            }
        }
    }

    private void roundTransform() {
        for (int i = 0; i < xArr.length; i++) {
            xArr[i] = roundTo5(xArr[i]);
        }
    }

    private boolean isGeoDist() {
        Kurtosis k = new Kurtosis();
        return k.evaluate(xArr) >= 3.0;
    }

    private double formatDouble(double x, int digits) {
        double base = Math.pow(10, (double) digits);
        return Math.round(x * base) / base;
    }

    private double roundTo(double x, int sigDigits) {
        if (x == 0) {
            return 0;
        }
        else {
            int digits = sigDigits - 1 - ((int) Math.floor(Math.log10(Math.abs(x))));
            return formatDouble(x, digits);
        }
    }

    private double roundTo5(double x) {
        if (Math.abs(x) <= 10) {
            return roundTo(x, 1);
        }
        String xStr = String.valueOf((int)roundTo(x, 2));
        int secondDigit = x > 0 ? 1 : 2;
        Character[] ch1 = {'1', '2', '8', '9'};
        Set<Character> chSet1 = new HashSet<>(Arrays.asList(ch1));
        Character[] ch2 = {'3', '4', '6', '7'};
        Set<Character> chSet2 = new HashSet<>(Arrays.asList(ch2));
        if (chSet1.contains(xStr.charAt(secondDigit))) {
            return roundTo(x, 1);
        } else if (chSet2.contains(xStr.charAt(secondDigit))) {
            xStr = xStr.substring(0, secondDigit) + "5" + xStr.substring(secondDigit + 1);
        }
        return Integer.valueOf(xStr).doubleValue();
    }

}
