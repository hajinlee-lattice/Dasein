package com.latticeengines.domain.exposed.query;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.KryoUtils;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "property")
@JsonSubTypes({ //
        @Type(value = BucketRestriction.class, name = "bucketRestriction"),
        @Type(value = TransactionRestriction.class, name = "transactionRestriction"),
        @Type(value = DateRestriction.class, name = "dateRestriction"),
        @Type(value = MetricRestriction.class, name = "metricRestriction"),
        @Type(value = TimeRestriction.class, name = "timeRestriction"),
        @Type(value = ConcreteRestriction.class, name = "concreteRestriction"),
        @Type(value = ExistsRestriction.class, name = "existsRestriction"),
        @Type(value = LogicalRestriction.class, name = "logicalRestriction") })
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class Restriction implements GraphNode {

    Restriction() {
    }

    public static RestrictionBuilder builder() {
        return new RestrictionBuilder();
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public Restriction getDeepCopy() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        KryoUtils.write(bos, this);
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        return KryoUtils.read(bis, Restriction.class);
    }

}
