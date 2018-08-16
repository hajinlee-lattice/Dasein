package com.latticeengines.domain.exposed.pls;

import java.util.Date;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.workflow.JobStatus;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RatingModelDTO {

    public RatingModelDTO() {
    }

    public RatingModelDTO(RatingModel ratingModel) {
        this.setId(ratingModel.getId());
        this.setCreated(ratingModel.getCreated());
        this.setCreatedBy(ratingModel.getCreatedBy());
        this.setIteration(ratingModel.getIteration());
        this.setDerivedFromRatingModelId(ratingModel.getDerivedFromRatingModel());
        if (ratingModel instanceof RuleBasedModel) {
            this.setRatingModelAttributes(ratingModel.getRatingModelAttributes());
        }
        if (ratingModel instanceof AIModel) {
            this.setModelingJobStatus(((AIModel) ratingModel).getModelingJobStatus());
            this.setModelSummaryId(((AIModel) ratingModel).getModelSummaryId());
        }
    }

    @JsonProperty("id")
    private String id;

    @JsonProperty("iteration")
    private int iteration;

    @JsonProperty("created")
    private Date created;

    @JsonProperty("createdBy")
    private String createdBy;

    @JsonProperty("modelingJobStatus")
    private JobStatus modelingJobStatus;

    @JsonProperty("modelSummaryId")
    private String modelSummaryId;

    @JsonProperty("derivedFromRatingModelId")
    private String derivedFromRatingModelId;

    @JsonProperty("ratingmodel_attributes")
    private Set<AttributeLookup> ratingModelAttributes;

    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return this.id;
    }

    public void setIteration(int iteration) {
        this.iteration = iteration;
    }

    public int getIteration() {
        return this.iteration;
    }

    public void setCreated(Date time) {
        this.created = time;
    }

    public Date getCreated() {
        return this.created;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public String getCreatedBy() {
        return this.createdBy;
    }

    public void setModelingJobStatus(JobStatus status) {
        this.modelingJobStatus = status;
    }

    public JobStatus getModelingJobStatus() {
        return this.modelingJobStatus;
    }

    public void setModelSummaryId(String modelSummaryId) {
        this.modelSummaryId = modelSummaryId;
    }

    public String getModelSummaryId() {
        return this.modelSummaryId;
    }

    public String getDerivedFromRatingModelId() {
        return derivedFromRatingModelId;
    }

    public void setDerivedFromRatingModelId(String derivedFromRatingModelId) {
        this.derivedFromRatingModelId = derivedFromRatingModelId;
    }

    public Set<AttributeLookup> getRatingModelAttributes() {
        return this.ratingModelAttributes;
    }

    public void setRatingModelAttributes(Set<AttributeLookup> attributes) {
        this.ratingModelAttributes = attributes;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
