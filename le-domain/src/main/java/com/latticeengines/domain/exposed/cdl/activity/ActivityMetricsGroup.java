package com.latticeengines.domain.exposed.cdl.activity;

import java.io.Serializable;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.StringTemplate;
import com.latticeengines.domain.exposed.metadata.transaction.NullMetricsImputation;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "ACTIVITY_METRIC_GROUP", uniqueConstraints = {
        @UniqueConstraint(columnNames = {"GROUP_ID", "FK_TENANT_ID"})})
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ActivityMetricsGroup implements HasPid, HasTenant, Serializable {

    private static final long serialVersionUID = 0L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonProperty("pid")
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "GROUP_ID", nullable = false)
    @JsonProperty("groupId")
    private String groupId;

    @JsonProperty("groupName")
    @Column(name = "GROUP_NAME", nullable = false)
    private String groupName;

    @ManyToOne(cascade = CascadeType.REMOVE)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @ManyToOne(cascade = CascadeType.REMOVE)
    @JoinColumn(name = "FK_STREAM_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private AtlasStream stream;

    @JsonProperty("entity")
    @Enumerated(EnumType.STRING)
    @Column(name = "ENTITY", nullable = false)
    private BusinessEntity entity;

    @JsonProperty("rollupDimensions")
    @Column(name = "ROLLUP_DIMENSIONS", nullable = false)
    private String rollupDimensions; // comma separated dimension names

    @JsonProperty("aggregation")
    @Type(type = "json")
    @Column(name = "AGGREGATION", columnDefinition = "'JSON'", nullable = false)
    private StreamAttributeDeriver aggregation;

    @JsonProperty("timeRollup")
    @Type(type = "json")
    @Column(name = "TIME_ROLLUP", columnDefinition = "'JSON'", nullable = false)
    private ActivityTimeRange activityTimeRange;

    @JsonProperty("displayNameTmpl")
    @ManyToOne
    @JoinColumn(name = "DISPLAY_NAME_TMPL", nullable = false)
    @OnDelete(action = OnDeleteAction.NO_ACTION)
    private StringTemplate displayNameTmpl;

    @JsonProperty("descriptionTmpl")
    @ManyToOne
    @JoinColumn(name = "DESCRIPTION_TMPL")
    @OnDelete(action = OnDeleteAction.NO_ACTION)
    private StringTemplate descriptionTmpl;

    @JsonProperty("category")
    @Column(name = "CATEGORY", nullable = false)
    @Enumerated(EnumType.STRING)
    private Category category;

    @JsonProperty("subCategoryTmpl")
    @ManyToOne
    @JoinColumn(name = "SUBCATEGORY_TMPL", nullable = false)
    @OnDelete(action = OnDeleteAction.NO_ACTION)
    private StringTemplate subCategoryTmpl;

    @JsonProperty("javaClass")
    @Column(name = "JAVA_CLASS", nullable = false)
    private String javaClass;

    @JsonProperty("nullImputation")
    @Column(name = "NULL_IMP", nullable = false)
    @Enumerated(EnumType.STRING)
    private NullMetricsImputation nullImputation;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    @Override
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public AtlasStream getStream() {
        return stream;
    }

    public void setStream(AtlasStream stream) {
        this.stream = stream;
    }

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }

    public String getRollupDimensions() {
        return rollupDimensions;
    }

    public void setRollupDimensions(String rollupDimensions) {
        this.rollupDimensions = rollupDimensions;
    }

    public StreamAttributeDeriver getAggregation() {
        return aggregation;
    }

    public void setAggregation(StreamAttributeDeriver aggregation) {
        this.aggregation = aggregation;
    }

    public ActivityTimeRange getActivityTimeRange() {
        return activityTimeRange;
    }

    public void setActivityTimeRange(ActivityTimeRange activityTimeRange) {
        this.activityTimeRange = activityTimeRange;
    }

    public StringTemplate getDisplayNameTmpl() {
        return displayNameTmpl;
    }

    public void setDisplayNameTmpl(StringTemplate displayNameTmpl) {
        this.displayNameTmpl = displayNameTmpl;
    }

    public StringTemplate getDescriptionTmpl() {
        return descriptionTmpl;
    }

    public void setDescriptionTmpl(StringTemplate descriptionTmpl) {
        this.descriptionTmpl = descriptionTmpl;
    }

    public Category getCategory() {
        return category;
    }

    public void setCategory(Category category) {
        this.category = category;
    }

    public StringTemplate getSubCategoryTmpl() {
        return subCategoryTmpl;
    }

    public void setSubCategoryTmpl(StringTemplate subCategoryTmpl) {
        this.subCategoryTmpl = subCategoryTmpl;
    }

    public String getJavaClass() {
        return javaClass;
    }

    public void setJavaClass(String javaClass) {
        this.javaClass = javaClass;
    }

    public NullMetricsImputation getNullImputation() {
        return nullImputation;
    }

    public void setNullImputation(NullMetricsImputation nullImputation) {
        this.nullImputation = nullImputation;
    }
}
