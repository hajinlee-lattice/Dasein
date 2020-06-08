package com.latticeengines.domain.exposed.cdl.tray;

import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.FilterDef;
import org.hibernate.annotations.FilterDefs;
import org.hibernate.annotations.Filters;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.ParamDef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "TRAY_CONNECTOR_TEST", indexes = { @Index(name = "WORKFLOW_REQ_ID", columnList = "WORKFLOW_REQ_ID") })
@FilterDefs({ @FilterDef(name = "tenantFilter", defaultCondition = "TENANT_ID = :tenantFilterId", parameters = {
        @ParamDef(name = "tenantFilterId", type = "java.lang.Long") }) })
@Filters({ @Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId") })
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TrayConnectorTest implements HasPid, HasTenant {

    @Id
    @JsonProperty("pid")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("externalSystemName")
    @Column(name = "EXTERNAL_SYSTEM_NAME", nullable = false)
    @Enumerated(EnumType.STRING)
    private CDLExternalSystemName externalSystemName;

    @JsonProperty("testState")
    @Column(name = "TEST_STATE", nullable = false)
    @Enumerated(EnumType.STRING)
    private TestState testState;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @JsonProperty("testScenario")
    @Column(name = "TEST_SCENARIO")
    private String testScenario;

    @JsonProperty("workflowRequestId")
    @Column(name = "WORKFLOW_REQ_ID", unique = true, nullable = false)
    private String workflowRequestId;

    @JsonProperty("startTime")
    @Column(name = "START_TIME", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date startTime;

    @JsonProperty("endTime")
    @Column(name = "END_TIME", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date endTime;

    public CDLExternalSystemName getExternalSystemName() {
        return this.externalSystemName;
    }

    public void setCDLExternalSystemName(CDLExternalSystemName externalSystemName) {
        this.externalSystemName = externalSystemName;
    }

    public TestState getTestState() {
        return this.testState;
    }

    public void setTestState(TestState testState) {
        this.testState = testState;
    }

    public String getTestScenario() {
        return this.testScenario;
    }

    public void setTestScenario(String testScenario) {
        this.testScenario = testScenario;
    }

    public String getWorkflowRequestId() {
        return workflowRequestId;
    }

    public void setWorkflowRequestId(String workflowRequestId) {
        this.workflowRequestId = workflowRequestId;
    }

    public Date getStartTime() {
        return this.startTime;
    }

    public void setStartTime(Date eventStartedTime) {
        this.startTime = eventStartedTime;
    }

    public Date getEndTime() {
        return this.endTime;
    }

    public void setEventStartedTime(Date eventEndedTime) {
        this.endTime = eventEndedTime;
    }

    @Override
    public Tenant getTenant() {
        return this.tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

}
