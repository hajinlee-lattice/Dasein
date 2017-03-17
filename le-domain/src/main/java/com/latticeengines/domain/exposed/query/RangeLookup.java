package com.latticeengines.domain.exposed.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RangeLookup extends Lookup {
    @JsonProperty("range")
    private BucketRange range;

    public RangeLookup(Object min, Object max) {
        range = BucketRange.range(min, max);
    }

    public RangeLookup(BucketRange range) {
        this.range = range;
    }

    public RangeLookup() {
    }

    public BucketRange getRange() {
        return range;
    }

    public void setRange(BucketRange range) {
        this.range = range;
    }

    @Override
    public void accept(Visitor visitor, VisitorContext ctx) {
        visitor.visit(this, ctx);
    }
}
