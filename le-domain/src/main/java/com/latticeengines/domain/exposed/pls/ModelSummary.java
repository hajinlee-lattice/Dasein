package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
import java.util.List;

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
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

import org.apache.commons.lang.StringUtils;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Index;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasApplicationId;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.KeyValue;

@JsonIgnoreProperties(ignoreUnknown = true)
@Entity
@Table(name = "MODEL_SUMMARY", uniqueConstraints = { @UniqueConstraint(columnNames = { "ID" }),
        @UniqueConstraint(columnNames = { "NAME", "TENANT_ID" }) })
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class ModelSummary implements HasId<String>, HasName, HasPid, HasTenant, HasTenantId, HasApplicationId, Fact,
        Dimension {

    private String id;
    private String name;
    private String displayName;
    private Long pid;
    private Tenant tenant;
    private Long tenantId;
    private List<Predictor> predictors = new ArrayList<>();
    private Double rocScore;
    private String lookupId;
    private Boolean incomplete = false;
    private Boolean downloaded = false;
    private Boolean uploaded = false;
    private Long trainingRowCount;
    private Long testRowCount;
    private Long totalRowCount;
    private Long trainingConversionCount;
    private Long testConversionCount;
    private Long totalConversionCount;
    private KeyValue details;
    private Long constructionTime;
    private Long lastUpdateTime;
    private ModelSummaryStatus status = ModelSummaryStatus.INACTIVE;
    private String rawFile;
    private Double top10PercentLift;
    private Double top20PercentLift;
    private Double top30PercentLift;
    private String applicationId;
    private String eventTableName;
    private String sourceSchemaInterpretation;
    private String trainingTableName;
    private String transformationGroupName;
    private String predefinedSelectionName;
    private String predefinedSelectionVersion;
    private ColumnSelection customizedColumnSelection;
    private String pivotArtifactPath;
    private String modelType;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    @JsonProperty("Name")
    @Column(name = "NAME", nullable = false)
    @Index(name = "MODEL_SUMMARY_NAME_IDX")
    public String getName() {
        return name;
    }

    @Override
    @JsonProperty("Name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("DisplayName")
    @Column(name = "DISPLAY_NAME", nullable = true)
    public String getDisplayName() {
        return displayName;
    }

    @JsonProperty("DisplayName")
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @Override
    @JsonProperty("Id")
    @Column(name = "ID", unique = true, nullable = false)
    @Index(name = "MODEL_SUMMARY_ID_IDX")
    @MetricField(name = "ModelID")
    public String getId() {
        return id;
    }

    @Override
    @JsonProperty("Id")
    public void setId(String id) {
        this.id = id;
    }

    @Override
    @JsonProperty("Tenant")
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
        setTenantId(tenant.getPid());
    }

    @Override
    @JsonProperty("Tenant")
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    public Tenant getTenant() {
        return tenant;
    }

    public void addPredictor(Predictor predictor) {
        if (predictor != null) {
            predictors.add(predictor);
            predictor.setModelSummary(this);
            predictor.setTenantId(getTenantId());
        }
    }

    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY, mappedBy = "modelSummary")
    @OnDelete(action = OnDeleteAction.CASCADE)
    public List<Predictor> getPredictors() {
        return predictors;
    }

    public void setPredictors(List<Predictor> predictors) {
        this.predictors = predictors;
    }

    @Override
    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    public Long getTenantId() {
        return tenantId;
    }

    @Override
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @JsonProperty("RocScore")
    @Column(name = "ROC_SCORE", nullable = false)
    @Type(type = "com.latticeengines.db.exposed.extension.NaNSafeDoubleType")
    @MetricField(name = "RocScore", fieldType = MetricField.FieldType.DOUBLE)
    public Double getRocScore() {
        return rocScore;
    }

    @JsonProperty("RocScore")
    public void setRocScore(Double rocScore) {
        this.rocScore = rocScore;
    }

    @JsonProperty("LookupId")
    @Column(name = "LOOKUP_ID", nullable = false)
    public String getLookupId() {
        return lookupId;
    }

    @JsonProperty("LookupId")
    public void setLookupId(String lookupId) {
        this.lookupId = lookupId;
    }

    @JsonIgnore
    @Column(name = "DOWNLOADED", nullable = false)
    public Boolean getDownloaded() {
        return downloaded;
    }

    @JsonIgnore
    public void setDownloaded(Boolean downloaded) {
        this.downloaded = downloaded;
    }

    @JsonProperty("Uploaded")
    @Column(name = "UPLOADED", nullable = false)
    public Boolean isUploaded() {
        return uploaded;
    }

    @JsonProperty("Uploaded")
    public void setUploaded(Boolean uploaded) {
        this.uploaded = uploaded;
    }

    @JsonProperty("Incomplete")
    @Column(name = "INCOMPLETE", nullable = false)
    public Boolean isIncomplete() {
        return incomplete;
    }

    @JsonProperty("Incomplete")
    public void setIncomplete(Boolean incomplete) {
        this.incomplete = incomplete;
    }

    @JsonProperty("TrainingRowCount")
    @Column(name = "TRAINING_ROW_COUNT", nullable = false)
    public Long getTrainingRowCount() {
        return trainingRowCount;
    }

    @JsonProperty("TrainingRowCount")
    public void setTrainingRowCount(Long trainingRowCount) {
        this.trainingRowCount = trainingRowCount;
    }

    @JsonProperty("TestRowCount")
    @Column(name = "TEST_ROW_COUNT", nullable = false)
    public Long getTestRowCount() {
        return testRowCount;
    }

    @JsonProperty("TestRowCount")
    public void setTestRowCount(Long testRowCount) {
        this.testRowCount = testRowCount;
    }

    @JsonProperty("TotalRowCount")
    @Column(name = "TOTAL_ROW_COUNT", nullable = false)
    public Long getTotalRowCount() {
        return totalRowCount;
    }

    @JsonProperty("TotalRowCount")
    public void setTotalRowCount(Long totalRowCount) {
        this.totalRowCount = totalRowCount;
    }

    @JsonProperty("TrainingConversionCount")
    @Column(name = "TRAINING_CONVERSION_COUNT", nullable = false)
    public Long getTrainingConversionCount() {
        return trainingConversionCount;
    }

    @JsonProperty("TrainingConversionCount")
    public void setTrainingConversionCount(Long trainingConversionCount) {
        this.trainingConversionCount = trainingConversionCount;
    }

    @JsonProperty("TestConversionCount")
    @Column(name = "TEST_CONVERSION_COUNT", nullable = false)
    public Long getTestConversionCount() {
        return testConversionCount;
    }

    @JsonProperty("TestConversionCount")
    public void setTestConversionCount(Long testConversionCount) {
        this.testConversionCount = testConversionCount;
    }

    @JsonProperty("TotalConversionCount")
    @Column(name = "TOTAL_CONVERSION_COUNT", nullable = false)
    public Long getTotalConversionCount() {
        return totalConversionCount;
    }

    @JsonProperty("TotalConversionCount")
    public void setTotalConversionCount(Long totalConversionCount) {
        this.totalConversionCount = totalConversionCount;
    }

    @OneToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JoinColumn(name = "FK_KEY_VALUE_ID", nullable = false)
    @JsonProperty("Details")
    public KeyValue getDetails() {
        return details;
    }

    @JsonProperty("Details")
    public void setDetails(KeyValue details) {
        this.details = details;
        if (details != null) {
            details.setTenantId(getTenantId());
        }
    }

    @JsonProperty("ConstructionTime")
    @Column(name = "CONSTRUCTION_TIME", nullable = false)
    public Long getConstructionTime() {
        return constructionTime;
    }

    @JsonProperty("ConstructionTime")
    public void setConstructionTime(Long constructionTime) {
        this.constructionTime = constructionTime;
    }

    @JsonProperty("LastUpdateTime")
    @Column(name = "LAST_UPDATE_TIME", nullable = false)
    public Long getLastUpdateTime() {
        return lastUpdateTime;
    }

    @JsonProperty("LastUpdateTime")
    public void setLastUpdateTime(Long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    @JsonProperty("Status")
    @Column(name = "STATUS", nullable = false)
    @Enumerated(EnumType.ORDINAL)
    public ModelSummaryStatus getStatus() {
        return status;
    }

    public void setStatus(ModelSummaryStatus status) {
        this.status = status;
    }

    @Transient
    @JsonProperty("RawFile")
    public String getRawFile() {
        return rawFile;
    }

    @Transient
    @JsonProperty("RawFile")
    public void setRawFile(String rawFile) {
        this.rawFile = rawFile;
    }

    @JsonProperty("Top10PctLift")
    @Column(name = "TOP_10_PCT_LIFT", nullable = true)
    @Type(type = "com.latticeengines.db.exposed.extension.NaNSafeDoubleType")
    @MetricField(name = "Top10PercentLift", fieldType = MetricField.FieldType.DOUBLE)
    public Double getTop10PercentLift() {
        return top10PercentLift;
    }

    @JsonProperty("Top10PctLift")
    public void setTop10PercentLift(Double top10PercentLift) {
        this.top10PercentLift = top10PercentLift;
    }

    @JsonProperty("Top20PctLift")
    @Column(name = "TOP_20_PCT_LIFT", nullable = true)
    @Type(type = "com.latticeengines.db.exposed.extension.NaNSafeDoubleType")
    @MetricField(name = "Top20PercentLift", fieldType = MetricField.FieldType.DOUBLE)
    public Double getTop20PercentLift() {
        return top20PercentLift;
    }

    @JsonProperty("Top20PctLift")
    public void setTop20PercentLift(Double top20PercentLift) {
        this.top20PercentLift = top20PercentLift;
    }

    @JsonProperty("Top30PctLift")
    @Column(name = "TOP_30_PCT_LIFT", nullable = true)
    @Type(type = "com.latticeengines.db.exposed.extension.NaNSafeDoubleType")
    @MetricField(name = "Top30PercentLift", fieldType = MetricField.FieldType.DOUBLE)
    public Double getTop30PercentLift() {
        return top30PercentLift;
    }

    @JsonProperty("Top30PctLift")
    public void setTop30PercentLift(Double top30PercentLift) {
        this.top30PercentLift = top30PercentLift;
    }

    @JsonProperty("ApplicationId")
    @Column(name = "APPLICATION_ID", nullable = true)
    public String getApplicationId() {
        return applicationId;
    }

    @JsonProperty("ApplicationId")
    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    @JsonProperty("EventTableName")
    public void setEventTableName(String eventTableName) {
        this.eventTableName = eventTableName;
    }

    @JsonProperty("EventTableName")
    @Column(name = "EVENT_TABLE_NAME", nullable = true)
    public String getEventTableName() {
        return eventTableName;
    }

    @JsonProperty("SourceSchemaInterpretation")
    @Column(name = "SOURCE_SCHEMA_INTERPRETATION", nullable = true)
    public String getSourceSchemaInterpretation() {
        return sourceSchemaInterpretation;
    }

    public void setSourceSchemaInterpretation(String sourceSchemaInterpretation) {
        this.sourceSchemaInterpretation = sourceSchemaInterpretation;
    }

    @JsonProperty("TrainingTableName")
    @Column(name = "TRAINING_TABLE_NAME", nullable = true)
    public String getTrainingTableName() {
        return trainingTableName;
    }

    public void setTrainingTableName(String trainingTableName) {
        this.trainingTableName = trainingTableName;
    }

    @JsonProperty("TransformationGroupName")
    @Column(name = "TRANSFORMATION_GROUP_NAME", nullable = true)
    public String getTransformationGroupName() {
        return transformationGroupName;
    }

    @JsonProperty("TransformationGroupName")
    public void setTransformationGroupName(String transformationGroupName) {
        this.transformationGroupName = transformationGroupName;
    }

    @JsonProperty("PivotArtifactPath")
    @Column(name = "PIVOT_ARTIFACT_PATH", nullable = true)
    public String getPivotArtifactPath() {
        return pivotArtifactPath;
    }

    @JsonProperty("PivotArtifactPath")
    public void setPivotArtifactPath(String pivotArtifactPath) {
        this.pivotArtifactPath = pivotArtifactPath;
    }

    @JsonProperty("ModelType")
    @Column(name = "MODEL_TYPE", nullable = false)
    public String getModelType() {
        return modelType;
    }

    @JsonProperty("ModelType")
    public void setModelType(String modelType) {
        this.modelType = modelType;
    }

    @Transient
    @JsonProperty("PredefinedSelectionName")
    private String getPredefinedSelectionName() {
        return predefinedSelectionName;
    }

    @Transient
    @JsonProperty("PredefinedSelectionName")
    private void setPredefinedSelectionName(String predefinedSelectionName) {
        this.predefinedSelectionName = predefinedSelectionName;
    }

    @Transient
    @JsonProperty("PredefinedSelectionVersion")
    public String getPredefinedSelectionVersion() {
        return predefinedSelectionVersion;
    }

    @Transient
    @JsonProperty("PredefinedSelectionVersion")
    public void setPredefinedSelectionVersion(String predefinedSelectionVersion) {
        this.predefinedSelectionVersion = predefinedSelectionVersion;
    }

    @Transient
    @JsonProperty("CustomizedColumnSelection")
    public ColumnSelection getCustomizedColumnSelection() {
        return customizedColumnSelection;
    }

    @Transient
    @JsonProperty("CustomizedColumnSelection")
    public void setCustomizedColumnSelection(ColumnSelection customizedColumnSelection) {
        this.customizedColumnSelection = customizedColumnSelection;
    }

    @Transient
    @JsonIgnore
    public ColumnSelection.Predefined getPredefinedSelection() {
        if (StringUtils.isNotEmpty(getPredefinedSelectionName())) {
            return ColumnSelection.Predefined.fromName(getPredefinedSelectionName());
        } else {
            return null;
        }
    }

    @Transient
    @JsonIgnore
    public void setPredefinedSelection(ColumnSelection.Predefined predefinedSelection) {
        this.setPredefinedSelectionName(predefinedSelection.getName());
    }

    @Transient
    @JsonIgnore
    @MetricField(name = "CrossValidatedMean", fieldType = MetricField.FieldType.DOUBLE)
    public Double getCrossValidatedMean() {
        return getSummaryFieldDoubleValue("CrossValidatedMeanOfModelAccuracy");
    }

    @Transient
    @JsonIgnore
    @MetricField(name = "CrossValidatedStd", fieldType = MetricField.FieldType.DOUBLE)
    public Double getCrossValidatedStd() {
        return getSummaryFieldDoubleValue("CrossValidatedStdOfModelAccuracy");
    }

    private Double getSummaryFieldDoubleValue(String field) {
        if (getDetails() == null || getDetails().getPayload() == null) {
            return null;
        }
        String rawModelSummary = getDetails().getPayload();
        JsonNode modelSummaryJson = JsonUtils.deserialize(rawModelSummary, JsonNode.class);
        JsonNode std = modelSummaryJson.get(field);
        return std != null ? std.asDouble() : 0D;
    }

    @Transient
    @JsonIgnore
    public String getDataCloudVersion() {
        return getSummaryFieldStringValue("EventTableProvenance", "Data_Cloud_Version");
    }

    private String getSummaryFieldStringValue(String field1, String field2) {
        if (getDetails() == null || getDetails().getPayload() == null) {
            return null;
        }
        if (field1 == null || field2 == null) {
            return null;
        }
        String rawModelSummary = getDetails().getPayload();
        JsonNode modelSummaryJson = JsonUtils.deserialize(rawModelSummary, JsonNode.class);
        JsonNode field1Node = modelSummaryJson.get(field1);
        JsonNode field2Node = field1Node != null ? field1Node.get(field2) : null;; 
        return field2Node != null ? field2Node.textValue() : null;
    }

    // this annotation is not being called, for now I'll set lastUpdateTime
    // explicitly
    //
    // @PreUpdate
    // protected void onUpdate() {
    // lastUpdateTime = System.currentTimeMillis();
    // }

}
