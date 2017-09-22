package com.latticeengines.domain.exposed.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.graph.GraphNode;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class TransactionRestriction extends Restriction {

    @JsonProperty("productName")
    private String productName;

    @JsonProperty("productId")
    private String productId;

    @JsonProperty("timeRestriction")
    private TimeRestriction timeRestriction;

    @JsonProperty("negate")
    private boolean negate;

    @JsonProperty("spentRestriction")
    private PurchaseRestriction spentRestriction;

    @JsonProperty("unitRestriction")
    private PurchaseRestriction unitRestriction;

    public TransactionRestriction() {
    }

    public TransactionRestriction(String productName, String productId, TimeRestriction timeRestriction, boolean negate,
            PurchaseRestriction spentRestriction, PurchaseRestriction unitRestriction) {
        this.productName = productName;
        this.productId = productId;
        this.timeRestriction = timeRestriction;
        this.negate = negate;
        this.spentRestriction = spentRestriction;
        this.unitRestriction = unitRestriction;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public TimeRestriction getTimeRestriction() {
        return timeRestriction;
    }

    public void setTimeRestriction(TimeRestriction timeRestriction) {
        this.timeRestriction = timeRestriction;
    }

    public boolean isNegate() {
        return negate;
    }

    public void setNegate(boolean negate) {
        this.negate = negate;
    }

    public Collection<? extends GraphNode> getChildren() {
        List<Restriction> restrictions = new ArrayList<>();
        restrictions.add(timeRestriction);
        if (spentRestriction != null) {
            restrictions.add(spentRestriction);
        }
        if (unitRestriction != null) {
            restrictions.add(unitRestriction);
        }
        return restrictions;
    }

    @Override
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        Map<String, Collection<? extends GraphNode>> map = new HashMap<>();
        map.put("timeRestriction", Collections.singletonList(timeRestriction));
        if (spentRestriction != null) {
            map.put("spentRestriction", Collections.singletonList(spentRestriction));
        }
        if (unitRestriction != null) {
            map.put("unitRestriction", Collections.singletonList(unitRestriction));
        }
        return map;
    }
}
