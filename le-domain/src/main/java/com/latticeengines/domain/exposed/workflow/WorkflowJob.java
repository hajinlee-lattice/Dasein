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

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
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

    @Column(name = "APPLICATION_ID", nullable = true)
    private String applicationId;

    @Column(name = "WORKFLOW_ID", nullable = true)
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
            setInputContext(new HashMap<String, String>());
        }
        Map raw = JsonUtils.deserialize(inputContextString, Map.class);
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
