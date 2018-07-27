package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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
import javax.persistence.NamedAttributeNode;
import javax.persistence.NamedEntityGraph;
import javax.persistence.NamedQuery;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;

import org.hibernate.annotations.Filter;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

/**
 * Manages the webhook configuration generated for Marketo Integration
 *
 */
@Entity
@Table(name = "SCORING_REQUEST_CONFIG", indexes = {@Index(name="SCORING_REQUEST_CONFIG.REQ_CONFIG_ID", columnList="REQ_CONFIG_ID", unique=true)})
@NamedQuery(name = ScoringRequestConfig.NQ_FIND_CONFIGS_BY_CREDENTIAL_ID, query = ScoringRequestConfig.SELECT_CONFIG_SUMMARY_BY_CREDENTIAL)
@Filter(name = "tenantFilter", condition = "FK_TENANT_ID = :tenantFilterId")
@NamedEntityGraph(name = "ScoringRequestConfig.details", attributeNodes = { @NamedAttributeNode("marketoScoringMatchFields") })
public class ScoringRequestConfig extends ScoringRequestConfigSummary implements HasPid, HasTenant, HasAuditingFields {

    static final String SELECT_CONFIG_SUMMARY_BY_CREDENTIAL = "SELECT new com.latticeengines.domain.exposed.pls.ScoringRequestConfigSummary "
            + "( src.configId, src.modelUuid ) " 
            + "FROM ScoringRequestConfig src WHERE src.marketoCredential.pid = :credentialPid";
    
    public static final String NQ_FIND_CONFIGS_BY_CREDENTIAL_ID = "ScoringRequestConfig.findConfigsByCrendetialId";
    
    private Long pid;
    private Tenant tenant;
    private List<MarketoScoringMatchField> marketoScoringMatchFields = new ArrayList<>();
    private MarketoCredential marketoCredential;
    private String webhookResource;
    
    private Date created;
    private Date updated;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    @JsonProperty("pid")
    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }
    
    @Column(name = "REQ_CONFIG_ID", nullable = false)
    @Override
    public String getConfigId() {
        return super.getConfigId();
    }

    @Override
    public void setConfigId(String configId) {
        super.setConfigId(configId);
    }

    @Override
    @JsonIgnore
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    @OneToMany(cascade = CascadeType.ALL, mappedBy = "scoringRequestConfig", fetch = FetchType.LAZY)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("marketoScoringMatchFields")
    public List<MarketoScoringMatchField> getMarketoScoringMatchFields() {
        return marketoScoringMatchFields;
    }

    public void setMarketoScoringMatchFields(List<MarketoScoringMatchField> marketoScoringMatchFields) {
        this.marketoScoringMatchFields = marketoScoringMatchFields;
    }

    public void addMarketoScoringMatchField(MarketoScoringMatchField marketoScoringMatchField) {
        this.marketoScoringMatchFields.add(marketoScoringMatchField);
        marketoScoringMatchField.setScoringRequestConfig(this);
        marketoScoringMatchField.setTenant(this.getTenant());
    }

    @Column(name = "MODEL_UUID", nullable = false)
    @Override
    public String getModelUuid() {
        return super.getModelUuid();
    }

    @Override
    public void setModelUuid(String modelUuid) {
        super.setModelUuid(modelUuid);
    }

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @JoinColumn(name = "MARKETO_CREDENTIAL_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonIgnore
    public MarketoCredential getMarketoCredential() {
        return marketoCredential;
    }

    public void setMarketoCredential(MarketoCredential marketoCredential) {
        this.marketoCredential = marketoCredential;
    }

    @Transient
    @JsonProperty("webhookResource")
    public String getWebhookResource() {
        return webhookResource;
    }

    public void setWebhookResource(String webhookResource) {
        this.webhookResource = webhookResource;
    }

    @Override
    public void setCreated(Date time) {
        this.created = time;
    }

    @Override
    @JsonIgnore
    @Column(name = "CREATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    @CreatedDate
    public Date getCreated() {
        return this.created;
    }

    @Override
    public void setUpdated(Date time) {
        this.updated = time;
    }

    @Override
    @JsonIgnore
    @Column(name = "UPDATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    @LastModifiedDate
    public Date getUpdated() {
        return this.updated;
    }

    @Override
    public String toString() {
        return String.format("PID: %d, ConfigID: %s, ModelId: %s, Mapped Column Count: %d", this.pid, this.configId, this.modelUuid, this.marketoScoringMatchFields.size());
    }
    
}
