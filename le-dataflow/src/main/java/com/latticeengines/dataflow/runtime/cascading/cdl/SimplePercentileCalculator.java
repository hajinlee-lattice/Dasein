package com.latticeengines.dataflow.runtime.cascading.cdl;

import java.util.HashMap;
import java.util.Map;

public class SimplePercentileCalculator {
    private static double MINIMUM_LOWER_BOUND = 0.0;
    private static double MAXIMUM_UPPER_BOUND = 1.0;

    private int minPct;
    private int maxPct;

    private Map<Integer, Double> pctLowerMap;
    private Map<Integer, Double> pctUpperMap;

    public SimplePercentileCalculator(int minPct, int maxPct) {
        validatePctRange(minPct, maxPct);

        this.minPct = minPct;
        this.maxPct = maxPct;

        this.pctLowerMap = new HashMap<>();
        this.pctUpperMap = new HashMap<>();

        pctLowerMap.put(minPct, MINIMUM_LOWER_BOUND);
        pctUpperMap.put(maxPct, MAXIMUM_UPPER_BOUND);
    }

    private void validatePctRange(int minPct, int maxPct) {
        if (minPct < 1) {
            throw new IllegalArgumentException("Minimum percentile " + minPct + " not in [1, 99]");
        }

        if (maxPct > 99) {
            throw new IllegalArgumentException("Maximum percentile " + maxPct + " not in [1, 99]");
        }

        if (minPct > maxPct) {
            throw new IllegalArgumentException(
                    "Min percentile " + minPct + " can't be larger than max percentile " + maxPct);
        }
    }

    public int getMinPct() {
        return minPct;
    }

    public int getMaxPct() {
        return maxPct;
    }

    public Double getLowerBound(int pct) {
        return pctLowerMap.get(pct);
    }

    public Double getUpperBound(int pct) {
        return this.pctUpperMap.get(pct);
    }

    public void adjustBoundaries() {
        mendGaps();
        validateOrder();
        untieDuplicates();
        alignBoundaries();
    }

    private void validateOrder() {
        for (int pct = minPct; pct < maxPct; ++pct) {
            if (pctLowerMap.get(pct) > pctLowerMap.get(pct + 1)) {
                throw new IllegalArgumentException(
                        "Input data is not in descending order. Percentile calculation will be wrong.");
            }
        }
    }

    // align upper bound with lower bound of next pct
    private void alignBoundaries() {
        for (int pct = minPct; pct < maxPct; ++pct) {
            pctUpperMap.put(pct, pctLowerMap.get(pct + 1));
        }
    }

    // handle gaps when total number of elements is less than 100
    private void mendGaps() {
        for (int pct = minPct + 1; pct <= maxPct; ++pct) {
            if (pctLowerMap.get(pct) == null) {
                pctLowerMap.put(pct, pctLowerMap.get(pct - 1));
            }
        }
    }

    private int findNextValueIndex(int currentIndex) {
        Double currentValue = pctLowerMap.get(currentIndex);

        for (int index = currentIndex + 1; index <= maxPct; ++index) {
            Double nextValue = pctLowerMap.get(index);
            if (nextValue > currentValue) {
                return index;
            }
        }
        return -1;
    }

    private void untieDuplicates() {
        for (int index = minPct; index < maxPct; ++index) {

            Double currentValue = pctLowerMap.get(index);
            Double nextValue = pctLowerMap.get(index + 1);

            if (currentValue.equals(nextValue)) {
                int nextValueIndex = findNextValueIndex(index + 1);

                if (nextValueIndex == -1) {
                    nextValueIndex = maxPct + 1;
                    nextValue = MAXIMUM_UPPER_BOUND;
                } else {
                    nextValue = pctLowerMap.get(nextValueIndex);
                }

                int distance = nextValueIndex - index;
                double adjustment = (nextValue - currentValue) / distance;
                for (int i = 0; i < distance; ++i) {
                    pctLowerMap.put(index + i, currentValue + i * adjustment);
                }
            }
        }
    }

    public int compute(long total, long currentPos, double value) {
        // add padding if there are less than 100 elements
        double numElementsInBucket = (total < 100) ? 1. : total / 100.;
        double adjustment = (total < 100) ? (100. / total) : 1;

        int pct = maxPct - (int) (currentPos * adjustment / numElementsInBucket);

        if (pct < minPct) {
            pct = minPct;
        }

        pctLowerMap.putIfAbsent(pct, value);
        pctUpperMap.putIfAbsent(pct, value);

        if (value < pctLowerMap.get(pct)) {
            pctLowerMap.put(pct, value);
        }

        if (value > pctUpperMap.get(pct)) {
            pctUpperMap.put(pct, value);
        }

        return pct;
    }
}
