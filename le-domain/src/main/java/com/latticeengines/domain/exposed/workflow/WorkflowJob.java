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
import javax.persistence.JoinColumn;
import javax.persistence.Lob;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.persistence.Transient;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;
import com.latticeengines.domain.exposed.dataplatform.HasApplicationId;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
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

    @Column(name = "WORKFLOW_INPUT_CONTEXT")
    @Lob
    @Type(type = "org.hibernate.type.SerializableToBlobType")
    private Map<String, String> inputContext = new HashMap<>();

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
    public String getInputContextValue(String key) {
        return inputContext.get(key);
    }

    @Transient
    public void setInputContextValue(String key, String value) {
        inputContext.put(key, value);
    }

    @Transient
    public WorkflowExecutionId getAsWorkflowId() {
        if (workflowId != null) {
            return new WorkflowExecutionId(workflowId);
        } else {
            return null;
        }
    }

    public Map<String, String> getInputContext() {
        return inputContext;
    }

    public void setInputContex(Map<String, String> inputContext) {
        this.inputContext = inputContext;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        WorkflowJob other = (WorkflowJob) obj;
        if (tenant == null) {
            if (other.tenant != null)
                return false;
        } else if (!tenant.equals(other.tenant))
            return false;
        if (workflowId == null) {
            if (other.workflowId != null)
                return false;
        } else if (!workflowId.equals(other.workflowId)) {
            return false;
        }
        if (applicationId == null) {
            if (other.applicationId != null)
                return false;
        } else if (!applicationId.equals(other.applicationId))
            return false;
        if (userId == null) {
            if (other.userId != null)
                return false;
        } else if (!userId.equals(other.userId))
            return false;
        return true;
    }
}
