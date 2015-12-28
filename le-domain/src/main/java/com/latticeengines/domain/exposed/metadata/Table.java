package com.latticeengines.domain.exposed.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Filters;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata.AttributeMetadata;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata.KV;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@javax.persistence.Table(name = "METADATA_TABLE", //
uniqueConstraints = { @UniqueConstraint(columnNames = { "TENANT_ID", "NAME", "TYPE" }) })
@Filters({ //
        @Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId"), //
        @Filter(name = "typeFilter", condition = "TYPE = :typeFilterId") })
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class Table implements HasPid, HasName, HasTenantId, GraphNode {

    private Long pid;
    private String name;
    private String displayName;
    private List<Attribute> attributes = new ArrayList<>();
    private Schema schema;
    private Tenant tenant;
    private Long tenantId;
    private List<Extract> extracts = new ArrayList<>();
    private PrimaryKey primaryKey;
    private LastModifiedKey lastModifiedKey;
    private TableType tableType;
    private Integer tableTypeCode;

    public Table() {
        setTableTypeCode(TableType.DATATABLE.getCode());
    }

    public Table(TableType tableType) {
        setTableTypeCode(tableType.getCode());
    }

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

    public void addAttribute(Attribute attribute) {
        attributes.add(attribute);
        attribute.setTable(this);
        attribute.setTenant(getTenant());
    }

    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY, mappedBy = "table")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("attributes")
    public List<Attribute> getAttributes() {
        return attributes;
    }

    @JsonProperty("attributes")
    public void setAttributes(List<Attribute> attributes) {
        this.attributes = attributes;
    }

    @JsonIgnore
    @Transient
    public Map<String, Attribute> getNameAttributeMap() {
        Map<String, Attribute> map = new HashMap<String, Attribute>();

        for (Attribute attribute : attributes) {
            map.put(attribute.getName(), attribute);
        }
        return map;
    }

    @JsonIgnore
    @Transient
    public Schema getSchema() {
        return schema;
    }

    @JsonIgnore
    @Transient
    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    @Override
    public void accept(Visitor visitor, VisitorContext ctx) {
        visitor.visit(this, ctx);
    }

    @JsonIgnore
    @Transient
    public String[] getAttributeNames() {
        String[] attrs = new String[attributes.size()];
        for (int i = 0; i < attributes.size(); i++) {
            attrs[i] = attributes.get(i).getName();
        }
        return attrs;
    }

    @Override
    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    public Long getTenantId() {
        return tenantId;
    }

    @Override
    @JsonIgnore
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @JsonIgnore
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;

        if (tenant != null) {
            setTenantId(tenant.getPid());
        }

    }

    @JsonIgnore
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    public Tenant getTenant() {
        return tenant;
    }

    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY, mappedBy = "table")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("extracts")
    public List<Extract> getExtracts() {
        return extracts;
    }

    @JsonIgnore
    public void addExtract(Extract extract) {
        extracts.add(extract);
        extract.setTable(this);
        extract.setTenant(getTenant());
    }

    public void setExtracts(List<Extract> extracts) {
        this.extracts = extracts;
    }

    @OneToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY, mappedBy = "table")
    @JsonProperty("primary_key")
    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    @JsonProperty("primary_key")
    public void setPrimaryKey(PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;
        if (primaryKey != null) {
            primaryKey.setTable(this);
        }
    }

    @Override
    @JsonIgnore
    @Transient
    public Collection<? extends GraphNode> getChildren() {
        List<GraphNode> children = new ArrayList<>();
        children.addAll(attributes);
        children.add(primaryKey);
        children.addAll(extracts);
        return children;
    }

    @Override
    @JsonIgnore
    @Transient
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        Map<String, Collection<? extends GraphNode>> map = new HashMap<>();
        map.put("primaryKey", Arrays.<GraphNode> asList(new GraphNode[] { primaryKey }));
        map.put("extracts", extracts);
        map.put("attributes", attributes);
        return map;
    }

    @OneToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY, mappedBy = "table")
    @JsonProperty("last_modified_key")
    public LastModifiedKey getLastModifiedKey() {
        return lastModifiedKey;
    }

    @JsonProperty("last_modified_key")
    public void setLastModifiedKey(LastModifiedKey lastModifiedKey) {
        this.lastModifiedKey = lastModifiedKey;
        if (lastModifiedKey != null) {
            lastModifiedKey.setTable(this);
        }
    }

    @Transient
    public ModelingMetadata getModelingMetadata() {
        ModelingMetadata metadata = new ModelingMetadata();
        List<AttributeMetadata> attrMetadata = new ArrayList<>();
        for (Attribute attr : getAttributes()) {
            AttributeMetadata attrMetadatum = new AttributeMetadata();

            attrMetadatum.setColumnName(attr.getName());
            attrMetadatum.setDisplayName(attr.getDisplayName());
            attrMetadatum.setApprovedUsage(attr.getApprovedUsage());
            attrMetadatum.setDescription(attr.getDescription());
            attrMetadatum.setStatisticalType(attr.getStatisticalType());
            attrMetadatum.setTags(attr.getTags());
            attrMetadatum.setDisplayDiscretizationStrategy(attr.getDisplayDiscretizationStrategy());
            attrMetadatum.setFundamentalType(attr.getFundamentalType());
            attrMetadatum.setExtensions(Arrays.<KV> asList(new KV[] { //
                    new KV("Category", attr.getCategory()), //
                            new KV("DataType", attr.getDataType()) }));
            attrMetadatum.setDataQuality(attr.getDataQuality());
            attrMetadatum.setDataSource(attr.getDataSource());

            attrMetadata.add(attrMetadatum);
        }
        metadata.setAttributeMetadata(attrMetadata);
        return metadata;
    }

    @Transient
    @JsonIgnore
    public TableType getTableType() {
        return tableType;
    }

    @Transient
    @JsonIgnore
    public void setTableType(TableType tableType) {
        this.tableType = tableType;
        this.tableTypeCode = tableType.getCode();
    }

    @Column(name = "TYPE", nullable = false)
    @JsonIgnore
    public Integer getTableTypeCode() {
        return tableTypeCode;
    }

    @JsonIgnore
    public void setTableTypeCode(Integer tableTypeCode) {
        this.tableTypeCode = tableTypeCode;
        setTableType(TableType.getTableTypeByCode(tableTypeCode));
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @Transient
    @JsonProperty("extracts_directory")
    public String getExtractsDirectory() {
        if (extracts.size() == 0) {
            return null;
        }

        String parentDir = null;
        for (Extract extract : extracts) {
            String[] tokens = StringUtils.split(extract.getPath(), "/");
            if (tokens == null) {
                return null;
            }
            StringBuilder extractParentDir = new StringBuilder("");
            if (tokens[tokens.length-1].endsWith(".avro")) {
                for (int i = 0; i < tokens.length - 1; i++) {
                    extractParentDir.append("/").append(tokens[i]);
                }
            }
            
            parentDir = extractParentDir.toString();
        }

        return parentDir;
    }

    @JsonProperty("extracts_directory")
    @Deprecated
    public void setExtractsDirectory(String s) {
        // pass
    }

}
