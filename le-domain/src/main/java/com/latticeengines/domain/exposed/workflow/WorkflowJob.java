package com.latticeengines.domain.exposed.workflow;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasApplicationId;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.exception.ErrorDetails;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "WORKFLOW_JOB", indexes = { //
        @Index(name = "IX_APPLICATION_ID", columnList = "APPLICATION_ID"), //
        @Index(name = "IX_WORKFLOW_ID", columnList = "WORKFLOW_ID") //
})
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkflowJob implements HasPid, HasTenantId, HasApplicationId {

    private static final Logger log = LoggerFactory.getLogger(WorkflowJob.class);

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "TENANT_ID", nullable = false)
    private Long tenantId;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @Column(name = "APPLICATION_ID")
    private String applicationId;

    @Column(name = "WORKFLOW_ID")
    private Long workflowId;

    @Column(name = "USER_ID")
    private String userId;

    @Column(name = "EMR_CLUSTER_ID", length = 20)
    private String emrClusterId;

    @Column(name = "INPUT_CONTEXT", length = 4000)
    private String inputContextString;

    @Column(name = "STATUS")
    private String status;

    @Column(name = "START_TIME")
    private Long startTimeInMillis;

    @Column(name = "ERROR_DETAILS")
    @Type(type = "text")
    private String errorDetailsString;

    @Column(name = "REPORT_CONTEXT", length = 4000)
    private String reportContextString;

    @Column(name = "OUTPUT_CONTEXT", length = 4000)
    private String outputContextString;

    @Column(name = "PARENT_JOB_ID")
    private Long parentJobId;

    @Column(name = "TYPE")
    private String type;

    @Column(name = "ERROR_CATEGORY")
    private String errorCategory;

    @Column(name = "STACK")
    private String stack;

    @Type(type = "json")
    @Column(name = "CONFIGURATION", columnDefinition = "'JSON'")
    private WorkflowConfiguration workflowConfiguration;

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

    public Tenant getTenant() {
        return tenant;
    }

    public void setTenant(Tenant tenant) {
        this.tenant = tenant;

        if (tenant != null) {
            setTenantId(tenant.getPid());
        }

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

    public String getEmrClusterId() {
        return emrClusterId;
    }

    public void setEmrClusterId(String emrClusterId) {
        this.emrClusterId = emrClusterId;
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
        if (StringUtils.isEmpty(inputContextString)) {
            log.debug("input context is empty.");
            setInputContext(new HashMap<>());
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

    @Transient
    public Map<String, String> getReportContext() {
        if (StringUtils.isEmpty(reportContextString)) {
            log.debug("report context is empty.");
            setReportContext(new HashMap<>());
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
        if (StringUtils.isEmpty(outputContextString)) {
            log.debug("output context is empty.");
            setOutputContext(new HashMap<>());
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

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Long getStartTimeInMillis() {
        return startTimeInMillis;
    }

    public int setStartTimeInMillis(Long startTimeInMillis) {
        if (startTimeInMillis == null) {
            return -1;
        } else {
            this.startTimeInMillis = startTimeInMillis;
            return 0;
        }
    }

    @Transient
    public ErrorDetails getErrorDetails() {
        if (StringUtils.isEmpty(errorDetailsString)) {
            log.debug("error details is empty.");
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

    public String getErrorDetailsString() {
        return errorDetailsString;
    }

    public Long getParentJobId() {
        return this.parentJobId;
    }

    public void setParentJobId(Long parentJobId) {
        this.parentJobId = parentJobId;
    }

    public String getType() {
        return this.type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    public String getErrorCategory() {
        return errorCategory;
    }

    public void setErrorCategory(String errorCategory) {
        this.errorCategory = errorCategory;
    }

    public String getStack() {
        return stack;
    }

    public void setStack(String stack) {
        this.stack = stack;
    }

    public WorkflowConfiguration getWorkflowConfiguration() {
        return workflowConfiguration;
    }

    public void setWorkflowConfiguration(WorkflowConfiguration workflowConfiguration) {
        this.workflowConfiguration = workflowConfiguration;
    }
}
