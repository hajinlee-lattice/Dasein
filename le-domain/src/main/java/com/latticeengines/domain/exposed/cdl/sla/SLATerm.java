package com.latticeengines.domain.exposed.cdl.sla;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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
import com.vladmihalcea.hibernate.type.json.JsonStringType;

@Entity
@Table(name = "SLA_TERM", uniqueConstraints = { //
        @UniqueConstraint(columnNames = { "SLATERM_TYPE", "FK_TENANT_ID" }),
        @UniqueConstraint(columnNames = { "TERM_NAME", "FK_TENANT_ID" })})
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@TypeDefs({ @TypeDef(name = "json", typeClass = JsonStringType.class) })
public class SLATerm implements HasPid, HasTenant, HasAuditingFields, Serializable {

    private static final long serialVersionUID = 0L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "TERM_PID", unique = true, nullable = false)
    @JsonProperty("pid")
    private Long pid;

    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @Column(name = "TERM_NAME", nullable = false)
    @JsonProperty("term_name")
    private String termName;

    @JsonProperty("schedule_cron")
    @Column(name = "SCHEDULE_CRON")
    private String scheduleCron;

    @JsonProperty("delivery_duration")
    @Column(name = "DELIVERY_DURATION")
    private String deliveryDuration;

    @JsonProperty("created")
    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "CREATED", nullable = false)
    private Date created;

    @JsonProperty("updated")
    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "UPDATED", nullable = false)
    private Date updated;

    //list term is 'or' logic
    @JsonProperty("action_predicates")
    @Column(name = "PREDICATES", columnDefinition = "'JSON'")
    @Type(type = "json")
    private List<ActionPredicate> predicates = new ArrayList<>();

    @Column(name="SLATERM_TYPE")
    @JsonProperty("slaterm_type")
    @Enumerated(EnumType.STRING)
    private SLATermType slaTermType;

    @Column(name = "TIME_ZONE")
    @JsonProperty("time_zone")
    private String timeZone = "UTC";

    @Column(name = "FEATURE_FLAGS", columnDefinition = "'JSON'")
    @JsonProperty("feature_flags")
    @Type(type = "json")
    private List<String> featureFlags;

    @Column(name = "VERSION")
    @JsonProperty("version")
    private int version;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public Date getCreated() {
        return created;
    }

    @Override
    public void setCreated(Date date) {
        this.created = date;
    }

    @Override
    public Date getUpdated() {
        return updated;
    }

    @Override
    public void setUpdated(Date date) {
        this.updated = date;
    }

    @Override
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public String getScheduleCron() {
        return scheduleCron;
    }

    public void setScheduleCron(String scheduleCron) {
        this.scheduleCron = scheduleCron;
    }

    public String getDeliveryDuration() {
        return deliveryDuration;
    }

    public void setDeliveryDuration(String deliveryDuration) {
        this.deliveryDuration = deliveryDuration;
    }

    public List<ActionPredicate> getPredicates() {
        return predicates;
    }

    public void setPredicates(List<ActionPredicate> predicates) {
        this.predicates = predicates;
    }

    public void addPredicate(ActionPredicate predicate) {
        this.predicates.add(predicate);
    }

    public SLATermType getSlaTermType() {
        return slaTermType;
    }

    public void setSlaTermType(SLATermType slaTermType) {
        this.slaTermType = slaTermType;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public List<String> getFeatureFlags() {
        return featureFlags;
    }

    public void setFeatureFlags(List<String> featureFlags) {
        this.featureFlags = featureFlags;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public String getTermName() {
        return termName;
    }

    public void setTermName(String termName) {
        this.termName = termName;
    }

    public static final class Builder {
        private SLATerm term;

        public Builder() {
            term = new SLATerm();
        }

        public Builder withTenant(Tenant tenant) {
            term.setTenant(tenant);
            return this;
        }

        public Builder withTermName(String termName) {
            term.setTermName(termName);
            return this;
        }

        public Builder withScheduleCron(String scheduleCron) {
            term.setScheduleCron(scheduleCron);
            return this;
        }

        public Builder withDeliveryDuration(String deliveryDuration) {
            term.setDeliveryDuration(deliveryDuration);
            return this;
        }

        public Builder withPredicates(List<ActionPredicate> predicates) {
            term.setPredicates(predicates);
            return this;
        }

        public Builder addPredicate(ActionPredicate predicate) {
            term.addPredicate(predicate);
            return this;
        }

        public Builder withTimeZone(String timeZone) {
            term.setTimeZone(timeZone);
            return this;
        }

        public Builder withSLATermType(SLATermType slaTermType) {
            term.setSlaTermType(slaTermType);
            return this;
        }

        public Builder withFeatureFlags(List<String> featureFlags) {
            term.setFeatureFlags(featureFlags);
            return this;
        }

        public Builder withVersion(int version) {
            term.setVersion(version);
            return this;
        }

        public SLATerm build() {
            return term;
        }
    }

    public enum SLATermType {
        Daily, Weekly, Month
    }
}
