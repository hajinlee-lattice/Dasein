package com.latticeengines.domain.exposed.metadata;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Filters;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Lists;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata.AttributeMetadata;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata.KV;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.FieldSource;
import com.latticeengines.domain.exposed.scoringapi.FieldType;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
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

    private static final Log log = LogFactory.getLog(Table.class);

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
    private String interpretation;
    private boolean markedForPurge;
    private List<DataRule> dataRules = new ArrayList<>();

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
    public Attribute getAttribute(final String name) {
        if (name == null) {
            return null;
        }
        return Iterables.find(attributes, new Predicate<Attribute>() {
            @Override
            public boolean apply(@Nullable Attribute attribute) {
                if (attribute.getName() == null) {
                    return false;
                }
                return attribute.getName().equals(name);
            }
        }, null);
    }

    @JsonIgnore
    public Attribute getAttribute(final InterfaceName interfaceName) {
        if (interfaceName == null) {
            return null;
        }
        return Iterables.find(attributes, new Predicate<Attribute>() {

            @Override
            public boolean apply(@Nullable Attribute attribute) {
                return interfaceName == attribute.getInterfaceName();
            }
        }, null);
    }

    @JsonIgnore
    public void deduplicateAttributeNames() {
        Set<String> nameSet = new HashSet<String>();
        for (int i = 0; i < attributes.size(); i++) {
            Attribute attribute = attributes.get(i);
            String rootAttributeName = attribute.getName();
            String possibleName = rootAttributeName;
            int version = 0;
            while (nameSet.contains(possibleName)) {
                possibleName = String.format(rootAttributeName + "_%d", ++version);
            }
            nameSet.add(possibleName);
            if (version > 0) {
                attribute.setName(possibleName);
                log.info(String.format("Replacing %s with %s.", rootAttributeName, possibleName));
            }
        }
    }

    @JsonIgnore
    public Attribute getAttributeFromDisplayName(final String displayName) {
        if (displayName == null) {
            return null;
        }
        return Iterables.find(attributes, new Predicate<Attribute>() {
            @Override
            public boolean apply(@Nullable Attribute attribute) {
                if (attribute.getDisplayName() == null) {
                    return false;
                }
                return attribute.getDisplayName().equals(displayName);
            }
        }, null);
    }

    @JsonIgnore
    public List<Attribute> getAttributes(final LogicalDataType logicalDataType) {
        if (logicalDataType == null) {
            return null;
        }
        return findAttributes(new Predicate<Attribute>() {

            @Override
            public boolean apply(@Nullable Attribute attribute) {
                return attribute.getLogicalDataType() == logicalDataType;
            }
        });
    }

    @JsonIgnore
    public List<Attribute> findAttributes(Predicate<Attribute> predicate) {
        return Lists.newArrayList(Iterables.filter(attributes, predicate));
    }

    /**
     * Uses SchemaInterpretation enumeration
     */
    @Column(name = "INTERPRETATION")
    @JsonProperty("interpretation")
    public String getInterpretation() {
        return interpretation;
    }

    @JsonProperty("interpretation")
    public void setInterpretation(String interpretation) {
        this.interpretation = interpretation;
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

    @Column(name = "MARKED_FOR_PURGE", nullable = false)
    @JsonProperty("marked_for_purge")
    public boolean isMarkedForPurge() {
        return markedForPurge;
    }

    public void setMarkedForPurge(boolean markedForPurge) {
        this.markedForPurge = markedForPurge;
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
            attrMetadatum.setDataType(attr.getPhysicalDataType());
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

    @SuppressWarnings("unchecked")
    @Transient
    public Map.Entry<Map<String, FieldSchema>, List<TransformDefinition>> getRealTimeTransformationMetadata() {
        List<TransformDefinition> rtsTransforms = new ArrayList<>();
        Map<String, FieldSchema> fields = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();
        for (Attribute attr : getAttributes()) {
            if (!attr.getRTSAttribute()) {
                continue;
            }
            Map<String, Object> args;
            try {
                args = mapper.readValue(attr.getRTSArguments(), Map.class);
                TransformDefinition transform = new TransformDefinition(attr.getRTSModuleName(), //
                        attr.getName(), FieldType.getFromAvroType(attr.getPhysicalDataType()), args);
                rtsTransforms.add(transform);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        Set<String> requestTargets = new HashSet<>();
        for (Attribute attr : getAttributes()) {
            requestTargets.add(attr.getName());
        }
        for (TransformDefinition rtsTransform : rtsTransforms) {
            String output = rtsTransform.output;
            boolean equal = false;
            for (Object argument : rtsTransform.arguments.values()) {
                String argumentAsString = (String) argument;
                if (output.equals(argumentAsString)) {
                    equal = true;
                }
            }
            if (!equal) {
                requestTargets.remove(output);
            }
        }
        for (Attribute attr : getAttributes()) {
            boolean isRequest = requestTargets.contains(attr.getName());
            if (isRequest) {
                if (attr.getInterfaceName() == null
                        && (attr.getApprovedUsage() == null || attr.getApprovedUsage().size() == 0 || attr
                                .getApprovedUsage().get(0).equals("None"))) {
                    // Custom field with no approved usage
                    continue;
                } else if (LogicalDataType.isExcludedFromRealTimeMetadata(attr.getLogicalDataType())) {
                    continue;
                }
            }
            fields.put(attr.getName(), createField(attr, isRequest));
        }
        return new AbstractMap.SimpleEntry<>(fields, rtsTransforms);
    }

    private FieldSchema createField(Attribute attr, boolean request) {
        // Same logic as datacompositiongenerator.py
        FieldSchema fieldSchema = new FieldSchema();

        List<String> tags = attr.getTags();
        if (tags != null && !tags.isEmpty() && tags.get(0).equals(ModelingMetadata.EXTERNAL_TAG)) {
            fieldSchema.source = FieldSource.PROPRIETARY;
        } else if (request) {
            fieldSchema.source = FieldSource.REQUEST;
        } else {
            fieldSchema.source = FieldSource.TRANSFORMS;
        }

        FieldInterpretation interpretation;
        if (attr.getInterfaceName() != null) {
            switch (attr.getInterfaceName()) {
            case Id:
                interpretation = FieldInterpretation.Id;
                break;
            case Event:
                interpretation = FieldInterpretation.Event;
                break;
            case Domain:
                interpretation = FieldInterpretation.Domain;
                break;
            case FirstName:
                interpretation = FieldInterpretation.FirstName;
                break;
            case LastName:
                interpretation = FieldInterpretation.LastName;
                break;
            case Title:
                interpretation = FieldInterpretation.Title;
                break;
            case Email:
                interpretation = FieldInterpretation.Email;
                break;
            case City:
                interpretation = FieldInterpretation.City;
                break;
            case State:
                interpretation = FieldInterpretation.State;
                break;
            case PostalCode:
                interpretation = FieldInterpretation.PostalCode;
                break;
            case Country:
                interpretation = FieldInterpretation.Country;
                break;
            case PhoneNumber:
                interpretation = FieldInterpretation.PhoneNumber;
                break;
            case Website:
                interpretation = FieldInterpretation.Website;
                break;
            case CompanyName:
                interpretation = FieldInterpretation.CompanyName;
                break;
            case Industry:
                interpretation = FieldInterpretation.Industry;
                break;
            default:
                interpretation = FieldInterpretation.Feature;
                break;
            }
        } else {
            interpretation = FieldInterpretation.Feature;
        }
        if (attr.getLogicalDataType() == LogicalDataType.Date) {
            fieldSchema.type = FieldType.STRING;
            interpretation = FieldInterpretation.Date;
        } else {
            String avroType = attr.getPhysicalDataType();
            fieldSchema.type = FieldType.getFromAvroType(avroType.toLowerCase());
        }
        fieldSchema.interpretation = interpretation;

        log.info(String.format(
                "Added field to realtime metadata -- Name:%s ApprovedUsage:%s LogicalDataType:%s FieldSchema:%s",
                Strings.nullToEmpty(attr.getName()), StringUtils.join(attr.getApprovedUsage(), ","),
                attr.getLogicalDataType(), JsonUtils.serialize(fieldSchema)));

        return fieldSchema;
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

    @JsonProperty("data_rules")
    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY, mappedBy = "table")
    @OnDelete(action = OnDeleteAction.CASCADE)
    public List<DataRule> getDataRules() {
        return dataRules;
    }

    @JsonProperty("data_rules")
    public void setDataRules(List<DataRule> dataRules) {
        this.dataRules = dataRules;
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
            if (tokens[tokens.length - 1].endsWith(".avro")) {
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
