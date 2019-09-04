package com.latticeengines.domain.exposed.playmakercore;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.apache.avro.Schema;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.FilterDef;
import org.hibernate.annotations.FilterDefs;
import org.hibernate.annotations.Filters;
import org.hibernate.annotations.ParamDef;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.playmaker.PlaymakerUtils;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.SoftDeletable;
import com.latticeengines.domain.exposed.security.HasTenantId;

@Entity
@Table(name = "Recommendation", indexes = { //
        @Index(name = "REC_EXTERNAL_ID", columnList = "EXTERNAL_ID"), //
        @Index(name = "REC_PLAY_ID", columnList = "PLAY_ID"), //
        @Index(name = "REC_LAUNCH_ID", columnList = "LAUNCH_ID"), //
        @Index(name = "REC_LAUNCH_DATE", columnList = "LAUNCH_DATE"), //
        @Index(name = "REC_LAUNCH_LAST_UPD_TIME", columnList = "LAST_UPDATED_TIMESTAMP"), //
        @Index(name = "REC_DELETED", columnList = "DELETED"), //
        @Index(name = "REC_TENANT_ID", columnList = "TENANT_ID"), //
        @Index(name = "DESTINATION_SYS_TYPE", columnList = "DESTINATION_SYS_TYPE"), //
        @Index(name = "DESTINATION_ORG_ID", columnList = "DESTINATION_ORG_ID"), //
})
@JsonIgnoreProperties(ignoreUnknown = true)
@FilterDefs({
        @FilterDef(name = "tenantFilter", defaultCondition = "TENANT_ID = :tenantFilterId", parameters = {
                @ParamDef(name = "tenantFilterId", type = "java.lang.Long") }),
        @FilterDef(name = "softDeleteFilter", defaultCondition = "DELETED !=true") })
@Filters({ @Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId"),
        @Filter(name = "softDeleteFilter", condition = "DELETED != true") })
public class Recommendation implements HasPid, HasId<String>, HasTenantId, SoftDeletable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "EXTERNAL_ID", nullable = false)
    @JsonProperty("external_id")
    private String recommendationId;

    @Column(name = "ACCOUNT_ID", nullable = true)
    @JsonProperty("accountId")
    private String accountId;

    @Column(name = "LE_ACCOUNT_EXTERNAL_ID", nullable = false)
    @JsonProperty("leAccountExternalID")
    private String leAccountExternalID;

    @Column(name = "PLAY_ID", nullable = false)
    @JsonProperty("play_id")
    private String playId;

    @Column(name = "LAUNCH_ID", nullable = false)
    @JsonProperty("launch_id")
    private String launchId;

    @Column(name = "DESCRIPTION")
    @JsonProperty("description")
    private String description;

    @Column(name = "LAUNCH_DATE", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    @JsonProperty("launchDate")
    private Date launchDate;

    @Column(name = "LAST_UPDATED_TIMESTAMP", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    @JsonProperty("lastUpdatedTimestamp")
    private Date lastUpdatedTimestamp;

    @Column(name = "MONETARY_VALUE")
    @JsonProperty("monetaryValue")
    private Double monetaryValue;

    @Column(name = "LIKELIHOOD")
    @JsonProperty("likelihood")
    private Double likelihood;

    @Column(name = "COMPANY_NAME")
    @JsonProperty("companyName")
    private String companyName;

    @Column(name = "SFDC_ACCOUNT_ID")
    @JsonProperty("sfdcAccountID")
    private String sfdcAccountID;

    @Column(name = "PRIORITY_ID")
    @Enumerated(EnumType.STRING)
    @JsonProperty("priorityID")
    private RatingBucketName priorityID;

    @Column(name = "PRIORITY_DISPLAY_NAME")
    @JsonProperty("priorityDisplayName")
    private String priorityDisplayName;

    @Column(name = "MONETARY_VALUE_ISO4217_ID")
    @JsonProperty("monetaryValueIso4217ID")
    private String monetaryValueIso4217ID;

    @Lob
    @Column(name = "CONTACTS")
    @JsonProperty("CONTACTS")
    private String contacts;

    @Column(name = "SYNC_DESTINATION")
    @JsonProperty("synchronizationDestination")
    private String synchronizationDestination;

    @Column(name = "LIFT")
    @JsonProperty("lift")
    private Double lift;

    @Column(name = "RATING_MODEL_ID")
    @JsonProperty("rating_model_id")
    private String ratingModelId;

    @Column(name = "MODEL_SUMMARY_ID")
    @JsonProperty("modelSummaryId")
    private String modelSummaryId;

    @Column(name = "DESTINATION_ORG_ID")
    @JsonProperty("destinationOrgId")
    private String destinationOrgId;

    @Column(name = "DESTINATION_SYS_TYPE")
    @JsonProperty("destinationSysType")
    private String destinationSysType;

    @Column(name = "TENANT_ID", nullable = false)
    @JsonProperty("tenant_id")
    private Long tenantId;

    @JsonProperty("deleted")
    @Column(name = "DELETED", nullable = false)
    private Boolean deleted = Boolean.FALSE;

    public static List<Attribute> getSchemaAttributes() {
        List<Attribute> schema = new ArrayList<>();
        int index = 0;

        setAttribute(schema, index++, "PID", Schema.Type.LONG);
        setAttribute(schema, index++, "EXTERNAL_ID", Schema.Type.STRING);
        setAttribute(schema, index++, "ACCOUNT_ID", Schema.Type.STRING);
        setAttribute(schema, index++, "LE_ACCOUNT_EXTERNAL_ID", Schema.Type.STRING);
        setAttribute(schema, index++, "PLAY_ID", Schema.Type.STRING);
        setAttribute(schema, index++, "LAUNCH_ID", Schema.Type.STRING);
        setAttribute(schema, index++, "DESCRIPTION", Schema.Type.STRING);
        setAttribute(schema, index++, "LAUNCH_DATE", Schema.Type.LONG);
        setAttribute(schema, index++, "LAST_UPDATED_TIMESTAMP", Schema.Type.LONG);
        setAttribute(schema, index++, "MONETARY_VALUE", Schema.Type.DOUBLE);
        setAttribute(schema, index++, "LIKELIHOOD", Schema.Type.DOUBLE);
        setAttribute(schema, index++, "COMPANY_NAME", Schema.Type.STRING);
        setAttribute(schema, index++, "SFDC_ACCOUNT_ID", Schema.Type.STRING);
        setAttribute(schema, index++, "PRIORITY_ID", Schema.Type.STRING);
        setAttribute(schema, index++, "PRIORITY_DISPLAY_NAME", Schema.Type.STRING);
        setAttribute(schema, index++, "MONETARY_VALUE_ISO4217_ID", Schema.Type.STRING);
        setAttribute(schema, index++, "LIFT", Schema.Type.STRING);
        setAttribute(schema, index++, "RATING_MODEL_ID", Schema.Type.STRING);
        setAttribute(schema, index++, "MODEL_SUMMARY_ID", Schema.Type.STRING);
        setAttribute(schema, index++, "CONTACTS", Schema.Type.STRING);
        setAttribute(schema, index++, "SYNC_DESTINATION", Schema.Type.STRING);
        setAttribute(schema, index++, "DESTINATION_ORG_ID", Schema.Type.STRING);
        setAttribute(schema, index++, "DESTINATION_SYS_TYPE", Schema.Type.STRING);
        setAttribute(schema, index++, "TENANT_ID", Schema.Type.LONG);
        setAttribute(schema, index++, "DELETED", Schema.Type.BOOLEAN);

        return schema;
    }

    public static Map<String, Object> convertToMap(Recommendation rec) {
        Map<String, Object> recMap = null;
        if (rec != null) {
            Long launchTimestamp = rec.getLaunchDate() == null //
                    ? null
                    : rec.getLaunchDate().getTime();
            Long lastUpdatedTimestamp = rec.getLastUpdatedTimestamp() == null //
                    ? launchTimestamp
                    : rec.getLastUpdatedTimestamp().getTime();

            recMap = new HashMap<>();
            recMap.put("PID", rec.getPid());
            recMap.put("EXTERNAL_ID", rec.getId());
            recMap.put("ACCOUNT_ID", rec.getAccountId());
            recMap.put("LE_ACCOUNT_EXTERNAL_ID", rec.getLeAccountExternalID());
            recMap.put("PLAY_ID", rec.getPlayId());
            recMap.put("LAUNCH_ID", rec.getLaunchId());
            recMap.put("DESCRIPTION", rec.getDescription());
            recMap.put("LAUNCH_DATE", launchTimestamp);
            recMap.put("LAST_UPDATED_TIMESTAMP", lastUpdatedTimestamp);
            recMap.put("MONETARY_VALUE", rec.getMonetaryValue());
            recMap.put("LIKELIHOOD", rec.getLikelihood());
            recMap.put("COMPANY_NAME", rec.getCompanyName());
            recMap.put("SFDC_ACCOUNT_ID", rec.getSfdcAccountID());
            recMap.put("PRIORITY_ID", rec.getPriorityID() != null ? rec.getPriorityID().name() : null);
            recMap.put("PRIORITY_DISPLAY_NAME", rec.getPriorityDisplayName());
            recMap.put("MONETARY_VALUE_ISO4217_ID", rec.getMonetaryValueIso4217ID());
            recMap.put("LIFT", rec.getLift());
            recMap.put("RATING_MODEL_ID", rec.getRatingModelId());
            recMap.put("MODEL_SUMMARY_ID", rec.getModelSummaryId());
            recMap.put("CONTACTS", rec.getContacts());
            recMap.put("SYNC_DESTINATION", rec.getSynchronizationDestination());
            recMap.put("DESTINATION_ORG_ID", rec.getDestinationOrgId());
            recMap.put("DESTINATION_SYS_TYPE", rec.getDestinationSysType());
            recMap.put("TENANT_ID", rec.getTenantId());
            recMap.put("DELETED", rec.getDeleted());
        }

        return recMap;
    }

    private static void setAttribute(List<Attribute> schema, int index, String attrName, Schema.Type physicalType) {
        Attribute attr = new Attribute(attrName);
        attr.setDisplayName(attrName);
        attr.setPhysicalDataType(physicalType.name());
        schema.add(index, attr);
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getRecommendationId() {
        return recommendationId;
    }

    public void setRecommendationId(String recommendationId) {
        this.recommendationId = recommendationId;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getLeAccountExternalID() {
        return leAccountExternalID;
    }

    public void setLeAccountExternalID(String leAccountExternalID) {
        this.leAccountExternalID = leAccountExternalID;
    }

    public String getPlayId() {
        return playId;
    }

    public void setPlayId(String playId) {
        this.playId = playId;
    }

    public String getLaunchId() {
        return launchId;
    }

    public void setLaunchId(String launchId) {
        this.launchId = launchId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Date getLaunchDate() {
        return launchDate;
    }

    public void setLaunchDate(Date launchDate) {
        this.launchDate = launchDate;
    }

    public Date getLastUpdatedTimestamp() {
        return lastUpdatedTimestamp;
    }

    public void setLastUpdatedTimestamp(Date lastUpdatedTimestamp) {
        this.lastUpdatedTimestamp = lastUpdatedTimestamp;
    }

    public Double getMonetaryValue() {
        return monetaryValue;
    }

    public void setMonetaryValue(Double monetaryValue) {
        this.monetaryValue = monetaryValue;
    }

    public Double getLikelihood() {
        return likelihood;
    }

    public void setLikelihood(Double likelihood) {
        this.likelihood = likelihood;
    }

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public String getSfdcAccountID() {
        return sfdcAccountID;
    }

    public void setSfdcAccountID(String sfdcAccountID) {
        this.sfdcAccountID = sfdcAccountID;
    }

    public RatingBucketName getPriorityID() {
        return priorityID;
    }

    public void setPriorityID(RatingBucketName priorityID) {
        this.priorityID = priorityID;
    }

    public String getPriorityDisplayName() {
        return priorityDisplayName;
    }

    public void setPriorityDisplayName(String name) {
        priorityDisplayName = name;
    }

    public String getMonetaryValueIso4217ID() {
        return monetaryValueIso4217ID;
    }

    public void setMonetaryValueIso4217ID(String monetaryValueIso4217ID) {
        this.monetaryValueIso4217ID = monetaryValueIso4217ID;
    }

    public String getContacts() {
        return contacts;
    }

    public void setContacts(String contacts) {
        this.contacts = contacts;
    }

    public List<Map<String, String>> getExpandedContacts() {
        return PlaymakerUtils.getExpandedContacts(this.contacts);
    }

    public void setExpandedContacts(List<Map<String, String>> contactList) {
        this.contacts = JsonUtils.serialize(contactList);
    }

    public String getSynchronizationDestination() {
        return synchronizationDestination;
    }

    public void setSynchronizationDestination(String synchronizationDestination) {
        this.synchronizationDestination = synchronizationDestination;
    }

    public String getRatingModelId() {
        return ratingModelId;
    }

    public void setRatingModelId(String ratingModelId) {
        this.ratingModelId = ratingModelId;
    }

    public Double getLift() {
        return lift;
    }

    public void setLift(Double lift) {
        this.lift = lift;
    }

    public String getModelSummaryId() {
        return modelSummaryId;
    }

    public void setModelSummaryId(String modelSummaryId) {
        this.modelSummaryId = modelSummaryId;
    }

    public String getDestinationOrgId() {
        return destinationOrgId;
    }

    public void setDestinationOrgId(String destinationOrgId) {
        this.destinationOrgId = destinationOrgId;
    }

    public String getDestinationSysType() {
        return destinationSysType;
    }

    public void setDestinationSysType(String destinationSysType) {
        this.destinationSysType = destinationSysType;
    }

    @Override
    public Long getTenantId() {
        return tenantId;
    }

    @Override
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @Override
    public Boolean getDeleted() {
        return deleted;
    }

    @Override
    public void setDeleted(Boolean deleted) {
        this.deleted = deleted;
    }

    @Override
    public String getId() {
        return recommendationId;
    }

    @Override
    public void setId(String id) {
        this.recommendationId = id;
    }
}
