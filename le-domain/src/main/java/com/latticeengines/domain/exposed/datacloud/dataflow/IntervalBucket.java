package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.statistics.BucketType;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class IntervalBucket extends BucketAlgorithm {

    private static final long serialVersionUID = 5591535115795170400L;

    @JsonProperty("bnds")
    private List<Number> boundaries;

    @Override
    @JsonIgnore
    public String getAlgorithm() {
        return INTERVAL;
    }

    public List<Number> getBoundaries() {
        return boundaries;
    }

    public void setBoundaries(List<Number> boundaries) {
        this.boundaries = boundaries;
    }

    @Override
    @JsonIgnore
    public List<String> generateLabelsInternal() {
        List<String> labels = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(boundaries)) {
            Number firstBoundary = boundaries.get(0);
            String firstLabel = String.format("< %s", formatBoundary(firstBoundary));
            List<String> middleLabels = new ArrayList<>();
            if (boundaries.size() > 1) {
                for (int i = 0; i < boundaries.size() - 1; i++) {
                    Number lowerBound = boundaries.get(i);
                    Number upperBound = boundaries.get(i + 1);
                    middleLabels.add(String.format("%s - %s", formatBoundary(lowerBound),
                            formatBoundary(upperBound)));
                }
            }
            Number lastBoundary = boundaries.get(boundaries.size() - 1);
            String lastLabel = String.format(">= %s", formatBoundary(lastBoundary));

            labels.add(null);
            labels.add(firstLabel);
            labels.addAll(middleLabels);
            labels.add(lastLabel);
        }

        return labels;
    }

    private String formatBoundary(Number number) {
        long longValue;
        if (number instanceof Long || number instanceof Integer) {
            longValue = Long.valueOf(number.toString());
        } else if (Double.valueOf(number.toString()) == Double.valueOf(number.toString())
                .intValue()) {
            // convert to integer
            longValue = Double.valueOf(number.toString()).intValue();
            number = longValue;
        } else if (number instanceof Double || number instanceof Float) {
            return number.toString();
        } else {
            throw new IllegalArgumentException("Does not know how to format boundary " + number
                    + " of type " + number.getClass().getSimpleName());
        }
        String formatted = number.toString();
        if (longValue != 0 && longValue % 1000 == 0) {
            formatted = String.format("%dK", Long.valueOf(number.toString()) / 1000);
        }
        if (longValue != 0 && longValue % 1000_000 == 0) {
            formatted = String.format("%dM", Long.valueOf(number.toString()) / 1000000);
        }
        if (longValue != 0 && longValue % 1000_000_000 == 0) {
            formatted = String.format("%dB", Long.valueOf(number.toString()) / 1000_000_000);
        }
        return formatted;
    }

    @JsonIgnore
    @Override
    public BucketType getBucketType() {
        return BucketType.Numerical;
    }
}
