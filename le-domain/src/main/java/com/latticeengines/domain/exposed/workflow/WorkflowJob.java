package com.latticeengines.domain.exposed.workflow;

import java.util.HashMap;
import java.util.Map;

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
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.hibernate.annotations.Index;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasApplicationId;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.exception.ErrorDetails;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "WORKFLOW_JOB")
public class WorkflowJob implements HasPid, HasTenantId, HasApplicationId {

    private static final Logger log = LoggerFactory.getLogger(WorkflowJob.class);

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "TENANT_ID", nullable = false)
    private Long tenantId;

    @OneToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @Index(name = "IX_APPLICATION_ID")
    @Column(name = "APPLICATION_ID")
    private String applicationId;

    @Index(name = "IX_WORKFLOW_ID")
    @Column(name = "WORKFLOW_ID")
    private Long workflowId;

    @Column(name = "USER_ID")
    private String userId;

    @Column(name = "INPUT_CONTEXT", length = 4000)
    private String inputContextString;

    @Column(name = "STATUS")
    @Enumerated(EnumType.STRING)
    private FinalApplicationStatus status;

    @Column(name = "START_TIME")
    private Long startTimeInMillis;

    @Column(name = "ERROR_DETAILS")
    @Type(type = "text")
    private String errorDetailsString;

    @Column(name = "REPORT_CONTEXT", length = 4000)
    private String reportContextString;

    @Column(name = "OUTPUT_CONTEXT", length = 4000)
    private String outputContextString;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public Long getTenantId() {
        return tenantId;
    }

    @Override
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public void setTenant(Tenant tenant) {
        this.tenant = tenant;

        if (tenant != null) {
            setTenantId(tenant.getPid());
        }

    }

    public Tenant getTenant() {
        return tenant;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Long getWorkflowId() {
        return workflowId;
    }

    public void setWorkflowId(Long workflowId) {
        this.workflowId = workflowId;
    }

    @Transient
    public WorkflowExecutionId getAsWorkflowId() {
        if (workflowId != null) {
            return new WorkflowExecutionId(workflowId);
        } else {
            return null;
        }
    }

    @Transient
    public Map<String, String> getInputContext() {
        if (inputContextString == null) {
            log.info("input context is empty.");
            setInputContext(new HashMap<String, String>());
        }
        Map<?, ?> raw = JsonUtils.deserialize(inputContextString, Map.class);
        return JsonUtils.convertMap(raw, String.class, String.class);
    }

    @Transient
    public void setInputContext(Map<String, String> inputContext) {
        this.inputContextString = JsonUtils.serialize(inputContext);
    }

    @Transient
    public String getInputContextValue(String key) {
        Map<String, String> context = getInputContext();
        return context.get(key);
    }

    @Transient
    public void setInputContextValue(String key, String value) {
        Map<String, String> context = getInputContext();
        context.put(key, value);
        setInputContext(context);
    }

    public String getInputContextString() {
        return inputContextString;
    }

    public void setInputContextString(String inputContextString) {
        this.inputContextString = inputContextString;
    }

    @Transient
    public Map<String, String> getReportContext() {
        if (reportContextString == null) {
            log.info("report context is empty.");
            setReportContext(new HashMap<String, String>());
        }
        Map<?, ?> raw = JsonUtils.deserialize(reportContextString, Map.class);
        return JsonUtils.convertMap(raw, String.class, String.class);
    }

    @Transient
    public void setReportContext(Map<String, String> reportContext) {
        this.reportContextString = JsonUtils.serialize(reportContext);
    }

    @Transient
    public String getReportName(String reportPurpose) {
        Map<String, String> context = getReportContext();
        return context.get(reportPurpose);
    }

    @Transient
    public void setReportName(String reportPurpose, String reportName) {
        Map<String, String> context = getReportContext();
        context.put(reportPurpose, reportName);
        setReportContext(context);
    }

    public String getReportContextString() {
        return reportContextString;
    }

    public void setReportContextString(String reportContextString) {
        this.reportContextString = reportContextString;
    }

    @Transient
    public Map<String, String> getOutputContext() {
        if (outputContextString == null) {
            log.info("output context is empty.");
            setOutputContext(new HashMap<String, String>());
        }
        Map<?, ?> raw = JsonUtils.deserialize(outputContextString, Map.class);
        return JsonUtils.convertMap(raw, String.class, String.class);
    }

    @Transient
    public void setOutputContext(Map<String, String> outContext) {
        this.outputContextString = JsonUtils.serialize(outContext);
    }

    @Transient
    public String getOutputContextValue(String key) {
        Map<String, String> context = getOutputContext();
        return context.get(key);
    }

    @Transient
    public void setOutputContextValue(String key, String value) {
        Map<String, String> context = getOutputContext();
        context.put(key, value);
        setOutputContext(context);
    }

    public String getOutputContextString() {
        return outputContextString;
    }

    public void setOutputContextString(String outputContextString) {
        this.outputContextString = outputContextString;
    }

    public FinalApplicationStatus getStatus() {
        return status;
    }

    public void setStatus(FinalApplicationStatus status) {
        this.status = status;
    }

    public Long getStartTimeInMillis() {
        return startTimeInMillis;
    }

    public void setStartTimeInMillis(Long startTimeInMillis) {
        this.startTimeInMillis = startTimeInMillis;
    }

    @Transient
    public ErrorDetails getErrorDetails() {
        if (errorDetailsString == null) {
            return null;
        }
        return JsonUtils.deserialize(errorDetailsString, ErrorDetails.class);
    }

    @Transient
    public void setErrorDetails(ErrorDetails details) {
        if (details == null) {
            return;
        }
        errorDetailsString = JsonUtils.serialize(details);
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }
}
