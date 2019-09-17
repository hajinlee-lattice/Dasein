package com.latticeengines.domain.exposed.pls;

import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.NamedAttributeNode;
import javax.persistence.NamedEntityGraph;
import javax.persistence.NamedEntityGraphs;
import javax.persistence.Transient;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.AdvancedModelingConfig;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;
import com.latticeengines.domain.exposed.workflow.JobStatus;

import io.swagger.annotations.ApiModel;

@Entity
@javax.persistence.Table(name = "AI_MODEL")
@JsonIgnoreProperties(ignoreUnknown = true)
@OnDelete(action = OnDeleteAction.CASCADE)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@NamedEntityGraphs(value = { @NamedEntityGraph(name = "AIModel.details", attributeNodes = {
        @NamedAttributeNode("ratingEngine"), @NamedAttributeNode("trainingSegment") }) })
@ApiModel("Represents AIModel JSON Object")
public class AIModel extends RatingModel {
    private static final String AI_MODEL_PREFIX = "ai";
    private static final String AI_MODEL_FORMAT = "%s_%s";

    private PredictionType predictionType;

    private String modelingJobId;

    private JobStatus modelingJobStatus = JobStatus.PENDING;

    private MetadataSegment trainingSegment;

    private AdvancedModelingConfig advancedModelingConfig;

    private String modelSummaryId;

    public static String generateIdStr() {
        String uuid = AvroUtils.getAvroFriendlyString(UuidUtils.shortenUuid(UUID.randomUUID()));
        return String.format(AI_MODEL_FORMAT, AI_MODEL_PREFIX, uuid);
    }

    @JsonProperty("predictionType")
    @Column(name = "PREDICTION_TYPE", nullable = false)
    @Enumerated(EnumType.STRING)
    public PredictionType getPredictionType() {
        if (predictionType == null) {
            predictionType = PredictionType.PROPENSITY;
        }
        return predictionType;
    }

    public void setPredictionType(PredictionType predictionType) {
        this.predictionType = predictionType;
    }

    @JsonIgnore
    @Transient
    public ApplicationId getModelingYarnJobId() {
        return ApplicationIdUtils.toApplicationIdObj(modelingJobId);
    }

    @JsonProperty("modelingJobId")
    @Column(name = "MODELING_JOBID")
    public String getModelingJobId() {
        return modelingJobId;
    }

    public void setModelingJobId(String modelingJobId) {
        this.modelingJobId = modelingJobId;
    }

    @JsonProperty("modelingJobStatus")
    @Column(name = "MODELING_JOB_STATUS")
    public JobStatus getModelingJobStatus() {
        return modelingJobStatus;
    }

    public void setModelingJobStatus(JobStatus status) {
        modelingJobStatus = status;
    }

    @Lob
    @Column(name = "ADVANCED_MODELING_CONFIG")
    @JsonIgnore
    public String getAdvancedModelingConfigStr() {
        return JsonUtils.serialize(advancedModelingConfig);
    }

    public void setAdvancedModelingConfigStr(String advancedModelingConfig) {
        AdvancedModelingConfig advancedModelingConfigObj = null;
        if (advancedModelingConfig != null) {
            advancedModelingConfigObj = JsonUtils.deserialize(advancedModelingConfig, AdvancedModelingConfig.class);
        }
        this.advancedModelingConfig = advancedModelingConfigObj;
    }

    @Transient
    @JsonProperty("advancedModelingConfig")
    public AdvancedModelingConfig getAdvancedModelingConfig() {
        return advancedModelingConfig;
    }

    public void setAdvancedModelingConfig(AdvancedModelingConfig advancedModelingConfig) {
        this.advancedModelingConfig = advancedModelingConfig;
    }

    @JsonProperty("trainingSegment")
    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TRAINING_SEGMENT_ID", nullable = true)
    @OnDelete(action = OnDeleteAction.CASCADE)
    public MetadataSegment getTrainingSegment() {
        return trainingSegment;
    }

    public void setTrainingSegment(MetadataSegment trainingSegment) {
        this.trainingSegment = trainingSegment;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @Column(name = "MODEL_SUMMARY_ID")
    @JsonProperty("modelSummaryId")
    public String getModelSummaryId() {
        return modelSummaryId;
    }

    public void setModelSummaryId(String modelSummaryId) {
        this.modelSummaryId = modelSummaryId;
    }
}
