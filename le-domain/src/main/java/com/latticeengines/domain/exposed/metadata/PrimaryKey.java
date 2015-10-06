package com.latticeengines.domain.exposed.metadata;

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
import javax.persistence.OneToOne;
import javax.persistence.Transient;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;


@Entity
@javax.persistence.Table(name = "METADATA_PRIMARY_KEY")
@JsonIgnoreProperties({"hibernateLazyInitializer", "handler"})
public class PrimaryKey implements AttributeOwner {
    
    private Long pid;
    private String name;
    private String displayName;
    private List<String> attributes = new ArrayList<>();
    private Table table;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Column(name = "NAME", unique = false, nullable = false)
    @Override
    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @Override
    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @Column(name = "DISPLAY_NAME", nullable = false)
    @JsonProperty("display_name")
    public String getDisplayName() {
        return displayName;
    }

    @JsonProperty("display_name")
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @JsonProperty("attributes")
    @Transient
    @Override
    public List<String> getAttributes() {
        return attributes;
    }

    @JsonProperty("attributes")
    @Override
    public void setAttributes(List<String> attributes) {
        this.attributes = attributes;
    }

    @Override
    public void accept(Visitor visitor, VisitorContext ctx) {
        visitor.visit(this, ctx);
    }

    @Override
    @JsonIgnore
    @Transient
    public Collection<? extends GraphNode> getChildren() {
        return new ArrayList<>();
    }

    @Override
    @JsonIgnore
    @Transient
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        return new HashMap<>();
    }

    @JsonIgnore
    @Transient
    @Override
    public String[] getAttributeNames() {
        String[] attrs = new String[attributes.size()];
        attributes.toArray(attrs);
        return attrs;
    }

    @Override
    public void addAttribute(String attribute) {
        attributes.add(attribute);
    }

    @JsonIgnore
    @OneToOne(cascade = { CascadeType.MERGE, CascadeType.REMOVE })
    @JoinColumn(name = "FK_TABLE_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    public Table getTable() {
        return table;
    }
    
    @JsonIgnore
    public void setTable(Table table) {
        this.table = table;
    }
    
}
