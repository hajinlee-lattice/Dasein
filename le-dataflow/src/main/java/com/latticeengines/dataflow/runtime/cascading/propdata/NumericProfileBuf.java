package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.stat.descriptive.moment.Kurtosis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.IntervalBucket;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import edu.emory.mathcs.backport.java.util.Collections;

@SuppressWarnings("rawtypes")
public class NumericProfileBuf extends BaseOperation implements Buffer {

    private static final long serialVersionUID = -4591759360012525636L;

    private boolean equalSized;
    private int buckets;
    private int minBucketSize;
    private List<String> attrs;
    private Map<String, Class<Comparable>> cls;
    private Map<String, Integer> namePositionMap;
    private Map<String, DVal> attrDVals;
    private ObjectMapper om;

    private class DVal implements Serializable {
        private static final long serialVersionUID = -7945104848635769412L;

        private double[] vals;
        private double[] valsOri;

        public DVal() {
        }

        public double[] getVals() {
            return vals;
        }

        public void setVals(double[] vals) {
            this.vals = vals;
        }

        public double[] getValsOri() {
            return valsOri;
        }

        public void setValsOri(double[] valsOri) {
            this.valsOri = valsOri;
        }

    }

    public NumericProfileBuf(Fields fieldDecl, boolean equalSized, int buckets, int minBucketSize,
            List<String> attrs, Map<String, Class<Comparable>> cls) {
        super(fieldDecl);
        this.equalSized = equalSized;
        this.buckets = buckets;
        this.minBucketSize = minBucketSize;
        this.attrs = attrs;
        this.cls = cls;
        this.namePositionMap = getPositionMap(fieldDeclaration);
        this.attrDVals = new HashMap<>();
        for (String attr : attrs) {
            attrDVals.put(attr, new DVal());
        }
        this.om = new ObjectMapper();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Map<String, List<Object>> attrVals = new HashMap<>();
        for (String attr : attrs) {
            attrVals.put(attr, new ArrayList<>());
        }
        Iterator<TupleEntry> argIter = bufferCall.getArgumentsIterator();
        while (argIter.hasNext()) {
            TupleEntry args = argIter.next();
            for (String attr : attrs) {
                Object val = args.getObject(attr);
                if (val == null) {
                    continue;
                }
                attrVals.get(attr).add(val);
            }

        }
        for (String attr : attrs) {
            Collections.sort(attrVals.get(attr));
        }
        doubleTransform(attrVals);
        roundTransform();
        keepOriArr();
        for (String attr : attrs) {
            List<Integer> boundIdxes = equalSized ? findBoundIdxesEqualSized(attr) : findBoundIdxesByDist(attr);
            settleResult(bufferCall, attr, boundIdxes);
        }
    }

    private void settleResult(BufferCall bufferCall, String attr, List<Integer> boundIdxes) {
        DVal dVal = attrDVals.get(attr);
        double[] valsOri = dVal.getValsOri();
        List<Number> boundaries = new ArrayList<>();
        if (boundIdxes.size() <= 2) {
            return;
        }
        for (int i = 1; i < boundIdxes.size() - 1; i++) {
            boundaries.add(numberTransform(valsOri[boundIdxes.get(i)], attr));
        }
        if (boundaries.size() == 0) {
            return;
        }
        IntervalBucket bucket = new IntervalBucket();
        bucket.setBoundaries(boundaries);

        Tuple result = Tuple.size(getFieldDeclaration().size());
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_ATTRNAME), attr);
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_SRCATTR), attr);
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_DECSTRAT), null);
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_ENCATTR), null);
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_LOWESTBIT), null);
        result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_NUMBITS), null);
        try {
            result.set(namePositionMap.get(DataCloudConstants.PROFILE_ATTR_BKTALGO), om.writeValueAsString(bucket));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Fail to format IntervalBucket object to json", e);
        }
        bufferCall.getOutputCollector().add(result);
    }

    /**
     * Split into roughly equally sized buckets 
     * Genertes a list of small bins as a first step in bucketing. 
     * Bins are roughly equally-sized, built from a sorted list. 
     * If a possible boundary straddles the same value, make sure the same value either belongs to the left side bin, or to the right side bin, or forms a bin by itself. 
     * Returns: a list of index positions at boundaries of these bins
     */
    private List<Integer> findBoundIdxesEqualSized(String attr) {
        DVal dVal = attrDVals.get(attr);
        double[] vals = dVal.getVals();
        List<Integer> buckIdxes = new ArrayList<>();
        int divider = 3;
        int stepSize = vals.length / (buckets * divider);
        int numPtsInBin = stepSize * divider;
        // if there is not enough data, no bucketing is needed
        if (stepSize == 0) {
            buckIdxes.add(0);
            buckIdxes.add(vals.length);
            return buckIdxes;
        }
        // all other cases - make bins by step size
        buckIdxes.add(0);
        int posCurr = 0;
        while (posCurr < vals.length) {
            // move the position by stepSize
            posCurr = posCurr + stepSize;     
            // end iteration when reaching the end
            if (posCurr >= vals.length) {
                buckIdxes.add(vals.length);
                break;
            }
            // if the value at current position is the same as the starting value of the current bucket, 
            // or there are still not sufficient points to make a new bucket, 
            // or if the remaining buckets are not sufficient to make a new bucket, 
            // keep expanding current bucket
            else if (vals[posCurr] == vals[buckIdxes.get(buckIdxes.size() - 1)]
                    || posCurr - buckIdxes.get(buckIdxes.size() - 1) < numPtsInBin
                    || vals.length - posCurr < numPtsInBin) {
                continue;
            }
            // when a new bucket is possible, check the new boundary point
            // if values at each side of the boundary point are different
            // create a new bucket
            else if (vals[posCurr] > vals[posCurr - 1]) {
                buckIdxes.add(posCurr);
            }
            // potential boundary separates the same value into two bins
            // need to move it
            else {
                // find position before first appearance of boundary value
                int idxL = findWhereValueChanges(vals, posCurr, -1);
                // if there are enough points without boundary values to make a bucket, 
                // back up to the left of first occurence of boundary values
                // make a new bucket and start from there
                if (idxL + 1 - buckIdxes.get(buckIdxes.size() - 1) >= numPtsInBin) {
                    posCurr = idxL + 1;
                    buckIdxes.add(posCurr);
                } 
                // otherwise, merge all boundary value occrences into left bucket
                else {
                    posCurr = findWhereValueChanges(vals, posCurr, 1);
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
    private int findWhereValueChanges(double[] vals, int idx, int direction) {
        while (idx + direction > 0 && idx + direction < vals.length) {
            if (vals[idx + direction] == vals[idx]) {
                idx = idx + direction;
            }
            else {
                break;
            }
        }
        // return the index of first occurence of a different x value
        // if already at the end, return the end point
        return Math.min(Math.max(idx + direction, 0), vals.length - 1);
    }

    /**
     *genertes a list of small bins as a first step in bucketing.
     *bins are built from a sorted list.
     *returns: a list of index positions at boundaries of these bins
     */
    private List<Integer> findBoundIdxesByDist(String attr) {
        DVal dVal = attrDVals.get(attr);
        double[] vals = dVal.getVals();
        List<Integer> buckIdxes = new ArrayList<>();
        // if there is not enough data, no bucketing is needed
        if (vals.length <= minBucketSize) {
            buckIdxes.add(0);
            buckIdxes.add(vals.length);
            return buckIdxes;
        }
        // all other cases - make bins by range
        if (isGeoDist(vals)) {
            logTransform(vals);
        }
        double xMin = vals[0];
        double xMax = vals[vals.length - 1];
        double[] boundaries = new double[buckets - 1];
        for (int i = 1; i < buckets; i++) {
            boundaries[i - 1] = xMin + (xMax - xMin) * i / buckets;
        }
        buckIdxes.add(0);
        for (double b : boundaries) {
            int bIdx = bisectLeft(vals, b);
            if ((bIdx - buckIdxes.get(buckIdxes.size() - 1)) >= minBucketSize) {
                buckIdxes.add(bIdx);
            }
        }
        if (xMax > buckIdxes.get(buckIdxes.size() - 1) - minBucketSize) {
            buckIdxes.add(vals.length);
        }
        else {
            buckIdxes.set(buckIdxes.size() - 1, vals.length);
        }
        return buckIdxes;
    }

    private int bisectLeft(double[] vals, double key) {
        int bIdx = Arrays.binarySearch(vals, key);
        if (bIdx >= 0) {
            while (bIdx > 0 && vals[bIdx] == vals[bIdx - 1]) {
                bIdx--;
            }
        } else {
            bIdx = -(bIdx + 1);
        }
        return bIdx;
    }

    private void keepOriArr() {
        for (String attr : attrs) {
            DVal dVal = attrDVals.get(attr);
            for (int i = 0; i < dVal.getValsOri().length; i++) {
                dVal.getValsOri()[i] = dVal.getVals()[i];
            }
        }
    }

    private void doubleTransform(Map<String, List<Object>> attrVals) {
        for (String attr : attrs) {
            DVal dVal = attrDVals.get(attr);
            List<Object> vals = attrVals.get(attr);
            dVal.setVals(new double[vals.size()]);
            dVal.setValsOri(new double[vals.size()]);
            Class<Comparable> c = cls.get(attr);
            if (c.equals(Integer.class)) {
                for (int i = 0; i < vals.size(); i++) {
                    dVal.getVals()[i] = ((Integer) vals.get(i)).doubleValue();
                }
            } else if (c.equals(Long.class)) {
                for (int i = 0; i < vals.size(); i++) {
                    dVal.getVals()[i] = ((Long) vals.get(i)).doubleValue();
                }
            } else if (c.equals(Float.class)) {
                for (int i = 0; i < vals.size(); i++) {
                    dVal.getVals()[i] = ((Float) vals.get(i)).doubleValue();
                }
            } else if (c.equals(Double.class)) {
                for (int i = 0; i < vals.size(); i++) {
                    dVal.getVals()[i] = ((Double) vals.get(i)).doubleValue();
                }
            } else {
                throw new UnsupportedOperationException(
                        String.format("%s type is not supported in numeric profiling", attr));
            }
        }


    }

    private void logTransform(double[] vals) {
        for (int i = 0; i < vals.length; i++) {
            if (vals[i] > 0) {
                vals[i] = Math.log10(vals[i]);
            } else if (vals[i] < 0) {
                vals[i] = -Math.log10(-vals[i]);
            }
        }
    }

    private void roundTransform() {
        for (String attr : attrs) {
            DVal dVal = attrDVals.get(attr);
            for (int i = 0; i < dVal.getVals().length; i++) {
                dVal.getVals()[i] = roundTo5(dVal.getVals()[i]);
            }
        }
    }

    private Number numberTransform(double x, String attr) {
        Class<Comparable> c = cls.get(attr);
        if (c.equals(Integer.class)) {
            return (Number) Integer.valueOf((int) x);
        } else if (c.equals(Long.class)) {
            return (Number) Long.valueOf((long) x);
        } else if (c.equals(Float.class)) {
            return (Number) Float.valueOf((float) x);
        } else if (c.equals(Double.class)) {
            return (Number) Double.valueOf(x);
        } else {
            throw new UnsupportedOperationException(
                    String.format("%s type is not supported in numeric profiling", attr));
        }
    }

    private boolean isGeoDist(double[] vals) {
        Kurtosis k = new Kurtosis();
        return k.evaluate(vals) >= 3.0;
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
