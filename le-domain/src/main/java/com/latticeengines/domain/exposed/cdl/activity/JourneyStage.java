package com.latticeengines.domain.exposed.cdl.activity;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;
import com.vladmihalcea.hibernate.type.json.JsonBinaryType;
import com.vladmihalcea.hibernate.type.json.JsonStringType;

@Entity
@Table(name = "JOURNEY_STAGE", uniqueConstraints = { //
        @UniqueConstraint(columnNames = { "STAGE_NAME", "FK_TENANT_ID" }) })
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@TypeDefs({ @TypeDef(name = "json", typeClass = JsonStringType.class),
        @TypeDef(name = "jsonb", typeClass = JsonBinaryType.class) })
public class JourneyStage implements HasPid, HasTenant, Serializable, HasAuditingFields {

    private static final long serialVersionUID = 0L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonProperty("pid")
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne(cascade = CascadeType.REMOVE)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @Column(name = "STAGE_NAME", nullable = false)
    @JsonProperty("stage_name")
    private String stageName;

    @Column(name = "DISPLAY_NAME", nullable = false)
    @JsonProperty("display_name")
    private String displayName;

    @Column(name = "DESCRIPTION")
    @JsonProperty("description")
    private String description;

    @Column(name = "DISPLAY_COLOR_CODE", nullable = false)
    @JsonProperty("display_color_code")
    private String displayColorCode;

    @Column(name = "PRIORITY", nullable = false)
    @JsonProperty("priority")
    private int priority;

    @Column(name = "PREDICATES", columnDefinition = "'JSON'")
    @JsonProperty("predicates")
    @Type(type = "json")
    private List<JourneyStagePredicate> predicates;

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

    @Override
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public String getStageName() {
        return stageName;
    }

    public void setStageName(String stageName) {
        this.stageName = stageName;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public List<JourneyStagePredicate> getPredicates() {
        return predicates;
    }

    public void setPredicates(List<JourneyStagePredicate> predicates) {
        this.predicates = predicates;
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

    public String getDisplayColorCode() {
        return displayColorCode;
    }

    public void setDisplayColorCode(String displayColorCode) {
        this.displayColorCode = displayColorCode;
    }

    @Override
    public Date getCreated() { return created; }

    @Override
    public void setCreated(Date created) { this.created = created; }

    @Override
    public Date getUpdated() { return updated; }

    @Override
    public void setUpdated(Date updated) { this.updated = updated; }

    public static final class Builder {

        private JourneyStage journeyStage;

        public Builder() {
            journeyStage = new JourneyStage();
        }

        public Builder withTenant(Tenant tenant) {
            journeyStage.setTenant(tenant);
            return this;
        }

        public Builder withStageName(String stageName) {
            journeyStage.setStageName(stageName);
            return this;
        }

        public Builder withDisplayName(String displayName) {
            journeyStage.setDisplayName(displayName);
            return this;
        }

        public Builder withDescription(String description) {
            journeyStage.setDescription(description);
            return this;
        }

        public Builder withDisplayColorCode(String displayColorCode) {
            journeyStage.setDisplayColorCode(displayColorCode);
            return this;
        }

        public Builder withPriority(int priority) {
            journeyStage.setPriority(priority);
            return this;
        }

        public Builder withPredicates(List<JourneyStagePredicate> predicates) {
            journeyStage.setPredicates(predicates);
            return this;
        }

        public JourneyStage build() {
            return journeyStage;
        }
    }
}
