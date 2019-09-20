package com.latticeengines.domain.exposed.cdl.activity;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "ATLAS_DIMENSION", uniqueConstraints = { //
        @UniqueConstraint(columnNames = { "NAME", "FK_TENANT_ID" }) })
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Dimension implements HasPid, Serializable, HasAuditingFields {

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
    @Enumerated(EnumType.STRING)
    @Column(name = "NAME", length = 50, nullable = false)
    private InterfaceName name;

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
    @JsonProperty("stream")
    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_STREAM_ID", nullable = false)
    private Stream stream;

    // for dimension which is generated from catalog
    @JsonProperty("catalog")
    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_CATALOG_ID")
    private Catalog catalog;

    @JsonIgnore
    @Column(name = "GENERATOR", nullable = false, length = 1000)
    private String generatorConfig;

    // configuration to generate dimension value universe
    @JsonProperty("generator")
    @Transient
    private DimensionGenerator generator;

    @JsonIgnore
    @Column(name = "CALCULATOR", nullable = false, length = 1000)
    private String calculatorConfig;

    // configuration to parse/calculate dimension in stream
    @JsonProperty("calculator")
    @Transient
    private DimensionCalculator calculator;

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

    public InterfaceName getName() {
        return name;
    }

    public void setName(InterfaceName name) {
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

    public Stream getStream() {
        return stream;
    }

    public void setStream(Stream stream) {
        this.stream = stream;
    }

    public String getGeneratorConfig() {
        return generatorConfig;
    }

    public void setGeneratorConfig(String generatorConfig) {
        this.generatorConfig = generatorConfig;
        this.generator = JsonUtils.deserialize(generatorConfig, DimensionGenerator.class);
    }

    public DimensionGenerator getGenerator() {
        return generator;
    }

    public void setGenerator(DimensionGenerator generator) {
        this.generator = generator;
        this.generatorConfig = JsonUtils.serialize(generator);
    }

    public void setCalculatorConfig(String calculatorConfig) {
        this.calculatorConfig = calculatorConfig;
        this.calculator = JsonUtils.deserialize(calculatorConfig, DimensionCalculator.class);
    }

    public DimensionCalculator getCalculator() {
        return calculator;
    }

    public void setCalculator(DimensionCalculator calculator) {
        this.calculator = calculator;
        this.calculatorConfig = JsonUtils.serialize(calculator);
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

}
