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
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EntityListeners;
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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Filters;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
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
@EntityListeners(TableListener.class)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Table implements HasPid, HasName, HasTenantId, GraphNode {

    private static final Logger log = LoggerFactory.getLogger(Table.class);

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "NAME", nullable = false)
    @JsonProperty("name")
    private String name;

    @Column(name = "DISPLAY_NAME", nullable = false)
    @JsonProperty("display_name")
    private String displayName;

    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY, mappedBy = "table")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("attributes")
    private List<Attribute> attributes = new ArrayList<>();

    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY, mappedBy = "table")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("level_hierarchies")
    private List<LevelBasedHierarchy> hierarchies = new ArrayList<>();

    @JsonIgnore
    @Transient
    private Schema schema;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    private Long tenantId;

    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY, mappedBy = "table")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("extracts")
    private List<Extract> extracts = new ArrayList<>();

    @OneToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY, mappedBy = "table")
    @JsonProperty("primary_key")
    private PrimaryKey primaryKey;

    @OneToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY, mappedBy = "table")
    @JsonProperty("last_modified_key")
    private LastModifiedKey lastModifiedKey;

    @Transient
    @JsonIgnore
    private TableType tableType;

    @Column(name = "TYPE", nullable = false)
    @JsonIgnore
    private Integer tableTypeCode;

    @Column(name = "INTERPRETATION")
    @JsonProperty("interpretation")
    private String interpretation;

    @Column(name = "MARKED_FOR_PURGE", nullable = false)
    @JsonProperty("marked_for_purge")
    private boolean markedForPurge;

    @JsonProperty("data_rules")
    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY, mappedBy = "table")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private List<DataRule> dataRules = new ArrayList<>();

    @OneToOne(fetch = FetchType.LAZY, cascade = CascadeType.ALL, mappedBy = "table")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("storage_mechanism")
    private StorageMechanism storageMechanism = null;

    @JsonProperty("namespace")
    @Column(name = "NAMESPACE", nullable = true)
    private String namespace;

    @JsonProperty("count")
    @Transient
    private Long count;

    public Table() {
    }

    public Table(TableType tableType) {
        setTableTypeCode(tableType.getCode());
    }

    @PostConstruct
    public void postConstruct() {
        setTableTypeCode(TableType.DATATABLE.getCode());
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public void addAttribute(Attribute attribute) {
        attributes.add(attribute);
        attribute.setTable(this);
        attribute.setTenant(getTenant());
    }

    public void addAttributes(List<Attribute> attributes) {
        attributes.stream().forEach(attr -> addAttribute(attr));
    }

    public void removeAttribute(final String name) {
        if (name == null) {
            return;
        }
        attributes.removeIf(attr -> name.equals(attr.getName()));
    }

    public List<Attribute> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<Attribute> attributes) {
        this.attributes = attributes;
    }

    @JsonIgnore
    public Attribute getAttribute(final String name) {
        if (name == null) {
            return null;
        }
        return attributes.stream().filter(attr -> name.equals(attr.getName())).findFirst()
                .orElse(null);
    }

    @JsonIgnore
    public Attribute getAttribute(final InterfaceName interfaceName) {
        if (interfaceName == null) {
            return null;
        }
        return attributes.stream().filter(attr -> interfaceName.equals(attr.getInterfaceName()))
                .findFirst().orElse(null);
    }

    @JsonIgnore
    public void deduplicateAttributeNames() {
        Set<String> nameSet = new HashSet<String>();
        deduplicateAttributeNames(nameSet, false);
    }

    @JsonIgnore
    public void deduplicateAttributeNamesIgnoreCase() {
        Set<String> nameSet = new HashSet<String>();
        deduplicateAttributeNames(nameSet, true);
    }

    @JsonIgnore
    public void deduplicateAttributeNames(Set<String> nameSet, boolean ignoreCase) {
        for (int i = 0; i < attributes.size(); i++) {
            Attribute attribute = attributes.get(i);
            String rootAttributeName = attribute.getName();
            String possibleName = rootAttributeName;
            String possibleNameIgnoreCase = ignoreCase ? possibleName.toLowerCase() : possibleName;
            int version = 0;
            while (nameSet.contains(possibleNameIgnoreCase)) {
                possibleName = String.format(rootAttributeName + "_%d", ++version);
                possibleNameIgnoreCase = ignoreCase ? possibleName.toLowerCase() : possibleName;
            }
            nameSet.add(possibleNameIgnoreCase);
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
        return attributes.stream().filter(attr -> displayName.equals(attr.getDisplayName()))
                .findFirst().orElse(null);
    }

    @JsonIgnore
    public List<Attribute> getAttributes(final LogicalDataType logicalDataType) {
        if (logicalDataType == null) {
            return null;
        }
        return attributes.stream().filter(attr -> logicalDataType.equals(attr.getLogicalDataType()))
                .collect(Collectors.toList());
    }

    @JsonIgnore
    public List<Attribute> findAttributes(Predicate<Attribute> predicate) {
        return attributes.stream().filter(predicate).collect(Collectors.toList());
    }

    /**
     * Uses SchemaInterpretation enumeration
     */
    public String getInterpretation() {
        return interpretation;
    }

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
    public Long getTenantId() {
        return tenantId;
    }

    @Override
    @JsonIgnore
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @JsonIgnore
    public Tenant getTenant() {
        return tenant;
    }

    @JsonIgnore
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;

        if (tenant != null) {
            setTenantId(tenant.getPid());
        }
    }

    public List<Extract> getExtracts() {
        return extracts;
    }

    public void setExtracts(List<Extract> extracts) {
        this.extracts = extracts;
    }

    @JsonIgnore
    public void addExtract(Extract extract) {
        extracts.add(extract);
        extract.setTable(this);
        extract.setTenant(getTenant());
    }

    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;
        if (primaryKey != null) {
            primaryKey.setTable(this);
        }
    }

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

    public LastModifiedKey getLastModifiedKey() {
        return lastModifiedKey;
    }

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
                        attr.getName(), FieldType.getFromAvroType(attr.getPhysicalDataType()),
                        args);
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
                        && (attr.getApprovedUsage() == null || attr.getApprovedUsage().size() == 0
                                || attr.getApprovedUsage().get(0).equals("None"))) {
                    // Custom field with no approved usage
                    continue;
                } else if (LogicalDataType
                        .isExcludedFromRealTimeMetadata(attr.getLogicalDataType())) {
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
                case DUNS:
                    interpretation = FieldInterpretation.DUNS;
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

        if (log.isDebugEnabled()) {
            log.debug(String.format(
                    "Added field to realtime metadata -- Name:%s ApprovedUsage:%s LogicalDataType:%s FieldSchema:%s",
                    StringUtils.defaultIfBlank(attr.getName(), ""),
                    StringUtils.join(attr.getApprovedUsage(), ","), attr.getLogicalDataType(),
                    JsonUtils.serialize(fieldSchema)));
        }

        return fieldSchema;
    }

    public TableType getTableType() {
        return tableType;
    }

    public void setTableType(TableType tableType) {
        this.tableType = tableType;

        if (tableType != null) {
            this.tableTypeCode = tableType.getCode();
        }

    }

    public Integer getTableTypeCode() {
        return tableTypeCode;
    }

    public void setTableTypeCode(Integer tableTypeCode) {
        this.tableTypeCode = tableTypeCode;
        setTableType(TableType.getTableTypeByCode(tableTypeCode));
    }

    public List<DataRule> getDataRules() {
        return dataRules;
    }

    public void setDataRules(List<DataRule> dataRules) {
        this.dataRules = dataRules;
    }

    @Transient
    @JsonProperty("extracts_directory")
    public String getExtractsDirectory() {
        if (CollectionUtils.isEmpty(extracts)) {
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

    @Deprecated
    public void setExtractsDirectory(String s) {
        // pass
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public StorageMechanism getStorageMechanism() {
        return storageMechanism;
    }

    public void setStorageMechanism(StorageMechanism storageMechanism) {
        this.storageMechanism = storageMechanism;

        if (storageMechanism != null) {
            storageMechanism.setTable(this);
        }
    }

    public List<LevelBasedHierarchy> getHierarchies() {
        return hierarchies;
    }

    public void setHierarchies(List<LevelBasedHierarchy> hierarchies) {
        this.hierarchies = hierarchies;
    }

    @JsonIgnore
    public List<ColumnMetadata> getColumnMetadata() {
        List<Attribute> attributes = getAttributes();
        if (attributes == null) {
            return null;
        } else {
            return attributes.stream().map(Attribute::getColumnMetadata)
                    .collect(Collectors.toList());
        }
    }

    @JsonProperty
    public HdfsDataUnit toHdfsDataUnit(String alias) {
        if (StringUtils.isBlank(alias)) {
            alias = this.getName();
        }
        HdfsDataUnit unit = new HdfsDataUnit();
        unit.setName(alias);
        String globPath = this.getExtracts().get(0).getPath();
        if (!globPath.endsWith(".avro") && !globPath.endsWith(".parquet")) {
            globPath = PathUtils.toAvroGlob(globPath);
        }
        unit.setPath(globPath);
        if (globPath.endsWith(".parquet")) {
            unit.setDataFormat(DataUnit.DataFormat.PARQUET);
        }
        unit.setCount(this.getExtracts().get(0).getProcessedRecords());
        return unit;
    }
}
