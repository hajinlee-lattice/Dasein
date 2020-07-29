package com.latticeengines.domain.exposed.cdl.sla;

import java.io.Serializable;
import java.util.Map;

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
import javax.persistence.OneToOne;
import javax.persistence.Table;
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
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;
import com.vladmihalcea.hibernate.type.json.JsonStringType;

@Entity
@Table(name = "SLA_FULFILLMENT", uniqueConstraints = { //
        @UniqueConstraint(columnNames = { "FK_TERM_PID", "FK_PID" }) })
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@TypeDefs({ @TypeDef(name = "json", typeClass = JsonStringType.class) })
public class SLAFulfillment implements HasPid, HasTenant, Serializable {

    private static final long serialVersionUID = 0L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    @JsonProperty("pid")
    private Long pid;

    @OneToOne
    @JoinColumn(name = "FK_PID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("action")
    private Action action;

    @ManyToOne
    @JoinColumn(name = "FK_TERM_PID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private SLATerm term;

    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @Column(name = "SLA_VERSION")
    @JsonProperty("sla_version")
    private int slaVersion;

    @Column(name = "EARLIEST_KICKOFF_TIME")
    @JsonProperty("earliest_kickoff_time")
    private Long earliestKickoffTime;

    @Column(name = "DELIVERY_DEADLINE")
    @JsonProperty("delivery_deadline")
    private Long deliveryDeadline;

    //workflowPid -> PA start time
    @Column(name = "JOB_STARTTIME", columnDefinition = "'JSON'")
    @JsonProperty("job_starttime")
    @Type(type = "json")
    private Map<Long, Long> jobStartTime;

    @Column(name = "JOB_FAILEDTIME", columnDefinition = "'JSON'")
    @JsonProperty("job_failedtime")
    @Type(type = "json")
    //workflowPid -> PA start time
    private Map<Long, Long> jobFailedTime;

    //PA Success time
    @Column(name = "DELIVERED_TIME")
    @JsonProperty("delivered_time")
    private Long deliveredTime;

    @Column(name = "FULFILLMENT_STATUS")
    @JsonProperty("fulfillment_status")
    @Enumerated(EnumType.STRING)
    private SLAFulfillmentStatus fulfillmentStatus;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public Action getAction() {
        return action;
    }

    public void setAction(Action action) {
        this.action = action;
    }

    public SLATerm getTerm() {
        return term;
    }

    public void setTerm(SLATerm term) {
        this.term = term;
    }

    public int getSlaVersion() {
        return slaVersion;
    }

    public void setSlaVersion(int slaVersion) {
        this.slaVersion = slaVersion;
    }

    public Long getEarliestKickoffTime() {
        return earliestKickoffTime;
    }

    public void setEarliestKickoffTime(Long earliestKickoffTime) {
        this.earliestKickoffTime = earliestKickoffTime;
    }

    public Long getDeliveryDeadline() {
        return deliveryDeadline;
    }

    public void setDeliveryDeadline(Long deliveryDeadline) {
        this.deliveryDeadline = deliveryDeadline;
    }

    public Map<Long, Long> getJobStartTime() {
        return jobStartTime;
    }

    public void setJobStartTime(Map<Long, Long> jobStartTime) {
        this.jobStartTime = jobStartTime;
    }

    public Map<Long, Long> getJobFailedTime() {
        return jobFailedTime;
    }

    public void setJobFailedTime(Map<Long, Long> jobFailedTime) {
        this.jobFailedTime = jobFailedTime;
    }

    public Long getDeliveredTime() {
        return deliveredTime;
    }

    public void setDeliveredTime(Long deliveredTime) {
        this.deliveredTime = deliveredTime;
    }

    public SLAFulfillmentStatus getFulfillmentStatus() {
        return fulfillmentStatus;
    }

    public void setFulfillmentStatus(SLAFulfillmentStatus fulfillmentStatus) {
        this.fulfillmentStatus = fulfillmentStatus;
    }

    @Override
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    /**
     * Pending: starting state
     * Fulfilled: PA completed before deadline
     * Violated: PA completed after deadline or action not kicked off
     */
    public enum SLAFulfillmentStatus {
        Pending, Fulfilled, Violated
    }

    public class Builder {
        private SLAFulfillment fulfillment;

        public Builder() {
            fulfillment = new SLAFulfillment();
        }

        public Builder withAction(Action action) {
            fulfillment.setAction(action);
            return this;
        }

        public Builder withTerm(SLATerm term) {
            fulfillment.setTerm(term);
            return this;
        }

        public Builder withTenant(Tenant tenant) {
            fulfillment.setTenant(tenant);
            return this;
        }

        public Builder withSlaVersion(int version) {
            fulfillment.setSlaVersion(version);
            return this;
        }

        public Builder withEarliestKickoffTime(Long earliestKickoffTime) {
            fulfillment.setEarliestKickoffTime(earliestKickoffTime);
            return this;
        }

        public Builder withDeliveryDeadline(Long deliveryDeadline) {
            fulfillment.setDeliveryDeadline(deliveryDeadline);
            return this;
        }

        public Builder withjobStartTime(Map<Long, Long> jobStartTime) {
            fulfillment.setJobStartTime(jobStartTime);
            return this;
        }

        public Builder withJobFailedTime(Map<Long, Long> jobFailedTime) {
            fulfillment.setJobFailedTime(jobFailedTime);
            return this;
        }

        public Builder withDeliveredTime(Long deliveredTime) {
            fulfillment.setDeliveredTime(deliveredTime);
            return this;
        }

        public Builder withFulfillmentStatus(SLAFulfillmentStatus fulfillmentStatus) {
            fulfillment.setFulfillmentStatus(fulfillmentStatus);
            return this;
        }

        public SLAFulfillment build() {
            return fulfillment;
        }
    }
}
