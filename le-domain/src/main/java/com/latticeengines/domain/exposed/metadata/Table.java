package com.latticeengines.domain.exposed.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.PrimaryKeyJoinColumn;
import javax.persistence.Transient;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.util.JsonUtils;

@Entity
@javax.persistence.Table(name = "DATA_TABLE")
@PrimaryKeyJoinColumn(name = "PID")
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class Table extends AttributeOwner {

    private List<Extract> extracts = new ArrayList<>();
    private PrimaryKey primaryKey;
    private LastModifiedKey lastModifiedKey;

    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY, mappedBy = "table")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("extracts")
    public List<Extract> getExtracts() {
        return extracts;
    }

    @JsonIgnore
    public void addExtract(Extract extract) {
        extracts.add(extract);
    }

    public void setExtracts(List<Extract> extracts) {
        this.extracts = extracts;
    }

    @OneToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JoinColumn(name = "FK_PRIMARY_KEY", nullable = false)
    @JsonProperty("primary_key")
    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    @JsonProperty("primary_key")
    public void setPrimaryKey(PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;
    }

    @Override
    @JsonIgnore
    @Transient
    public Collection<? extends GraphNode> getChildren() {
        List<GraphNode> children = new ArrayList<>();
        children.addAll(super.getChildren());
        children.add(primaryKey);
        children.addAll(extracts);
        return children;
    }

    @Override
    @JsonIgnore
    @Transient
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        Map<String, Collection<? extends GraphNode>> map = super.getChildMap();
        map.put("primaryKey", Arrays.<GraphNode> asList(new GraphNode[] { primaryKey }));
        map.put("extracts", extracts);
        return map;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public LastModifiedKey getLastModifiedKey() {
        return lastModifiedKey;
    }

    public void setLastModifiedKey(LastModifiedKey lastModifiedKey) {
        this.lastModifiedKey = lastModifiedKey;
    }

}
