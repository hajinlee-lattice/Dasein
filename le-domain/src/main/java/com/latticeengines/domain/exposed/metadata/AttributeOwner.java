package com.latticeengines.domain.exposed.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.MappedSuperclass;
import javax.persistence.OneToOne;
import javax.persistence.Transient;

import org.apache.commons.lang.StringUtils;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@MappedSuperclass
public abstract class AttributeOwner implements HasPid, HasName, GraphNode {
    
    protected Long pid;
    protected String name;
    protected String displayName;
    private List<String> attributes = new ArrayList<>();
    protected Table table;

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

    @JsonIgnore
    @OneToOne
    @JoinColumn(name = "FK_TABLE_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    public Table getTable() {
        return table;
    }
    
    @JsonIgnore
    public void setTable(Table table) {
        this.table = table;
    }

    
    @JsonProperty("attributes")
    @Transient
    public List<String> getAttributes() {
        return attributes;
    }

    @JsonProperty("attributes")
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

    @JsonIgnore
    @Transient
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        return new HashMap<>();
    }

    @JsonIgnore
    @Transient
    public String[] getAttributeNames() {
        String[] attrs = new String[attributes.size()];
        attributes.toArray(attrs);
        return attrs;
    }

    public void addAttribute(String attribute) {
        attributes.add(attribute);
    }
    
    @Column(name = "ATTRIBUTES", nullable = false, length = 2048)
    public String getAttributesAsStr() {
        return StringUtils.join(attributes, ",");
    }
    
    public void setAttributesAsStr(String attrStr) {
        if (attrStr != null) {
            attributes = Arrays.asList(attrStr.split(","));
        }
    }

}
