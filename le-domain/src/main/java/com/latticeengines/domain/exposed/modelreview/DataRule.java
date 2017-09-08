package com.latticeengines.domain.exposed.modelreview;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.Transient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.metadata.Table;

@Entity
@javax.persistence.Table(name = "MODELREVIEW_DATARULE")
public class DataRule implements HasName, HasPid, Serializable, GraphNode {

    private static final long serialVersionUID = 4426148663250916349L;

    @JsonIgnore
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonIgnore
    @ManyToOne(cascade = { CascadeType.MERGE })
    @JoinColumn(name = "FK_TABLE_ID", nullable = false)
    private Table table;

    @JsonProperty
    @Column(name = "NAME", nullable = false)
    private String name;

    @JsonProperty
    @Column(name = "MANDATORY_REMOVAL", nullable = false)
    private boolean mandatoryRemoval;

    @JsonProperty
    @Column(name = "DISPLAY_NAME", nullable = false)
    private String displayName;

    @JsonProperty
    @Column(name = "DESCRIPTION", length = 4000, nullable = false)
    private String description;

    @JsonProperty
    @Column(name = "FLAGGED_COLUMNS", nullable = true)
    @Lob
    @org.hibernate.annotations.Type(type = "org.hibernate.type.SerializableToBlobType")
    private List<String> flaggedColumnNames = new ArrayList<>();

    @JsonProperty
    @Column(name = "PROPERTIES", nullable = true)
    @Lob
    @org.hibernate.annotations.Type(type = "org.hibernate.type.SerializableToBlobType")
    private Map<String, Object> properties = new HashMap<>();

    @JsonProperty
    @Column(name = "ENABLED", nullable = false)
    private boolean enabled;

    public DataRule() {
    }

    public DataRule(String name) {
        this.name = name;
        this.mandatoryRemoval = false;
        this.displayName = name;
        this.description = name;
        this.enabled = true;
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    public boolean hasMandatoryRemoval() {
        return mandatoryRemoval;
    }

    public void setMandatoryRemoval(boolean mandatoryRemoval) {
        this.mandatoryRemoval = mandatoryRemoval;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<String> getFlaggedColumnNames() {
        return flaggedColumnNames;
    }

    public void setFlaggedColumnNames(List<String> flaggedColumnNames) {
        this.flaggedColumnNames = flaggedColumnNames;
    }

    @Transient
    @JsonIgnore
    public Object getPropertyValue(String key) {
        return properties.get(key);
    }

    @Transient
    @JsonIgnore
    public void setPropertyValue(String key, Object value) {
        properties.put(key, value);
    }

    @Transient
    @JsonIgnore
    @SuppressWarnings("unchecked")
    public List<String> getCustomerPredictors() {
        if (!properties.containsKey("CustomerPredictors")) {
            return new ArrayList<String>();
        }
        return (List<String>) properties.get("CustomerPredictors");
    }

    @Transient
    @JsonIgnore
    @SuppressWarnings("unchecked")
    public void addCustomerPredictor(String customerPredictor) {
        if (!properties.containsKey("CustomerPredictors")) {
            properties.put("CustomerPredictors", new ArrayList<String>());
        }
        ((List<String>) properties.get("CustomerPredictors")).add(customerPredictor);
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties.clear();
        this.properties.putAll(properties);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public void accept(Visitor visitor, VisitorContext ctx) {
        visitor.visit(this, ctx);
    }

    @Transient
    @Override
    @JsonIgnore
    public Collection<? extends GraphNode> getChildren() {
        return new ArrayList<>();
    }

    @Transient
    @Override
    @JsonIgnore
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        return new HashMap<>();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DataRule other = (DataRule) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        return true;
    }

}
