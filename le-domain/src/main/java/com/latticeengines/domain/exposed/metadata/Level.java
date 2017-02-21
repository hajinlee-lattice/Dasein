package com.latticeengines.domain.exposed.metadata;

import javax.persistence.Column;
import javax.persistence.Entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@Entity
@javax.persistence.Table(name = "METADATA_LEVEL")
@JsonIgnoreProperties({"hibernateLazyInitializer", "handler"})
public class Level extends AttributeOwner {

    @Column(name = "ORDER_IN_HIERARCHY", nullable = false)
    @JsonProperty("order")
    private int orderInHierarchy;

    public int getOrderInHierarchy() {
        return orderInHierarchy;
    }

    public void setOrderInHierarchy(int orderInHierarchy) {
        this.orderInHierarchy = orderInHierarchy;
    }
}
