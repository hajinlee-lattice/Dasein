package com.latticeengines.domain.exposed.query;


import static com.latticeengines.domain.exposed.query.AggregateLookup.Aggregator.COUNT;
import static com.latticeengines.domain.exposed.query.AggregateLookup.Aggregator.MAX;
import static com.latticeengines.domain.exposed.query.AggregateLookup.Aggregator.MIN;
import static com.latticeengines.domain.exposed.query.AggregateLookup.Aggregator.SUM;

import java.util.Collection;
import java.util.Collections;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.util.JsonUtils;


@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class AggregateLookup extends Lookup {

    @JsonProperty("aggregator")
    private Aggregator aggregator;

    @JsonProperty("lookup")
    private Lookup lookup;

    @JsonProperty("alias")
    private String alias;

    public static AggregateLookup create(Lookup mixin, Aggregator aggregator) {
        switch(aggregator) {
            case COUNT:
                return AggregateLookup.count();
            case SUM:
                return AggregateLookup.sum(mixin);
            case MAX:
                return AggregateLookup.max(mixin);
            default:
                throw new UnsupportedOperationException("Unsupported aggregator " + aggregator);
        }
    }

    public static AggregateLookup count() {
        AggregateLookup lookup1 = new AggregateLookup();
        lookup1.setAggregator(COUNT);
        return lookup1;
    }

    public static AggregateLookup sum(Lookup mixin) {
        AggregateLookup lookup1 = new AggregateLookup();
        lookup1.setLookup(mixin);
        lookup1.setAggregator(SUM);
        return lookup1;
    }

    public static AggregateLookup max(Lookup mixin) {
        AggregateLookup lookup1 = new AggregateLookup();
        lookup1.setLookup(mixin);
        lookup1.setAggregator(MAX);
        return lookup1;
    }

    public static AggregateLookup min(Lookup mixin) {
        AggregateLookup lookup1 = new AggregateLookup();
        lookup1.setLookup(mixin);
        lookup1.setAggregator(MIN);
        return lookup1;
    }

    public Aggregator getAggregator() {
        return aggregator;
    }

    public void setAggregator(Aggregator aggregator) {
        this.aggregator = aggregator;
    }

    public Lookup getLookup() {
        return lookup;
    }

    public void setLookup(Lookup lookup) {
        this.lookup = lookup;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public AggregateLookup as(String alias) {
        setAlias(alias);
        return this;
    }

    public enum Aggregator {
        COUNT, //
        SUM, //
        MAX, //
        MIN
    }

    @Override
    public Collection<? extends GraphNode> getChildren() {
        if (lookup != null) {
            return Collections.singleton(lookup);
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}

