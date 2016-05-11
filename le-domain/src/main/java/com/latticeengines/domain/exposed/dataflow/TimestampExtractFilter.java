package com.latticeengines.domain.exposed.dataflow;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.Extract;

public class TimestampExtractFilter extends ExtractFilter {
    public class Range {
        public Range(Long min, Long max) {
            this.min = min;
            this.max = max;
        }

        public boolean inRange(long millisecondsUtc) {
            return (min == null || min <= millisecondsUtc) && (max == null || millisecondsUtc <= max);
        }

        @JsonProperty
        public Long min;

        @JsonProperty
        public Long max;
    }

    @JsonProperty
    private List<Range> validDateRanges = new ArrayList<>();

    public List<Range> getValidDateRanges() {
        return validDateRanges;
    }

    public void setValidDateRanges(List<Range> validDateRanges) {
        this.validDateRanges = validDateRanges;
    }

    public void addDateRange(Long min, Long max) {
        Range range = new Range(min, max);
        validDateRanges.add(range);
    }

    @Override
    public boolean allows(Extract extract) {
        for (Range range : validDateRanges) {
            if (range.inRange(extract.getExtractionTimestamp())) {
                return true;
            }
        }

        return false;
    }
}
