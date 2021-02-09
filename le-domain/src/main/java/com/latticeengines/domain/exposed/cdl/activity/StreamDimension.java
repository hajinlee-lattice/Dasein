package com.latticeengines.domain.exposed.cdl.activity;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;
import org.hibernate.annotations.TypeDef;
import org.hibernate.annotations.TypeDefs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.security.Tenant;
import com.vladmihalcea.hibernate.type.json.JsonBinaryType;
import com.vladmihalcea.hibernate.type.json.JsonStringType;

@Entity
@Table(name = "ATLAS_STREAM_DIMENSION", uniqueConstraints = { //
        @UniqueConstraint(columnNames = { "NAME", "FK_STREAM_ID", "FK_TENANT_ID" }) })
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@TypeDefs({ @TypeDef(name = "json", typeClass = JsonStringType.class),
        @TypeDef(name = "jsonb", typeClass = JsonBinaryType.class) })
public class StreamDimension implements HasPid, Serializable, HasAuditingFields {

    private static final long serialVersionUID = 832890192489559837L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonProperty("pid")
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    // dimension attribute name
    // option 1: existing attribute name from stream
    // option 2: a new attribute name for generated dimension (current use case:
    // dimension generated from WebVisitPathPatterns catalog)
    @JsonProperty("name")
    @Column(name = "NAME", length = 100, nullable = false)
    private String name;

    // if not provided by user, set displayName same as name
    @JsonProperty("display_name")
    @Column(name = "DISPLAY_NAME", length = 250, nullable = false)
    private String displayName;

    @JsonProperty("tenant")
    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    // dimension is for which stream
    @JsonIgnore
    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_STREAM_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private AtlasStream stream;

    // for dimension which is generated from catalog
    @JsonProperty("catalog")
    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_CATALOG_ID")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Catalog catalog;

    // configuration to generate dimension value universe
    @JsonProperty("generator")
    @Type(type = "json")
    @Column(name = "GENERATOR", columnDefinition = "'JSON'", nullable = false)
    private DimensionGenerator generator;

    // configuration to parse/calculate dimension in stream
    @JsonProperty("calculator")
    @Type(type = "json")
    @Column(name = "CALCULATOR", columnDefinition = "'JSON'", nullable = false)
    private DimensionCalculator calculator;

    @JsonProperty("usages")
    @Type(type = "json")
    @Column(name = "USAGES", columnDefinition = "'JSON'", nullable = false)
    private Set<Usage> usages;

    @JsonProperty("shouldReplace")
    @Column(name = "SHOULD_REPLACE")
    // if false, newly generated dimension map will be merged into the active version
    private Boolean shouldReplace = true;

    @JsonProperty("deriveConfig")
    @Type(type = "json")
    @Column(name = "DERIVE_CONFIG", columnDefinition = "'JSON'")
    private DeriveConfig deriveConfig;

    @Column(name = "CREATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    @JsonProperty("created")
    private Date created;

    @Column(name = "UPDATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    @JsonProperty("updated")
    private Date updated;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public void setCatalog(Catalog catalog) {
        this.catalog = catalog;
    }

    public AtlasStream getStream() {
        return stream;
    }

    public void setStream(AtlasStream stream) {
        this.stream = stream;
    }

    public DimensionGenerator getGenerator() {
        return generator;
    }

    public void setGenerator(DimensionGenerator generator) {
        this.generator = generator;
    }

    public DimensionCalculator getCalculator() {
        return calculator;
    }

    public void setCalculator(DimensionCalculator calculator) {
        this.calculator = calculator;
    }

    @JsonIgnore
    public void addUsages(Usage... usages) {
        if (usages == null) {
            return;
        }
        if (this.usages == null) {
            this.usages = new HashSet<>();
        }
        this.usages.addAll(Arrays.asList(usages));
    }

    @JsonIgnore
    public void removeUsages(Usage... usages) {
        if (this.usages == null || usages == null) {
            return;
        }
        this.usages.removeAll(Arrays.asList(usages));
    }

    public Set<Usage> getUsages() {
        return usages;
    }

    public void setUsages(Set<Usage> usages) {
        this.usages = usages;
    }

    public Boolean getShouldReplace() {
        return shouldReplace;
    }

    public void setShouldReplace(Boolean shouldReplace) {
        this.shouldReplace = shouldReplace;
    }

    public DeriveConfig getDeriveConfig() {
        return deriveConfig;
    }

    public void setDeriveConfig(DeriveConfig deriveConfig) {
        this.deriveConfig = deriveConfig;
    }

    @Override
    public Date getCreated() {
        return created;
    }

    @Override
    public void setCreated(Date created) {
        this.created = created;
    }

    @Override
    public Date getUpdated() {
        return updated;
    }

    @Override
    public void setUpdated(Date updated) {
        this.updated = updated;
    }

    /*
     * what types of processing dimension will be used
     */
    public enum Usage {
        Pivot, Dedup, Filter
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StreamDimension dimension = (StreamDimension) o;
        return Objects.equal(pid, dimension.pid) && Objects.equal(name, dimension.name)
                && Objects.equal(catalog, dimension.catalog) && Objects.equal(generator, dimension.generator)
                && Objects.equal(calculator, dimension.calculator) && Objects.equal(usages, dimension.usages);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, generator, calculator, usages);
    }
}
