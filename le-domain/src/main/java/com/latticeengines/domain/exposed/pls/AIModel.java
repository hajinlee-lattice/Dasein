package com.latticeengines.domain.exposed.pls;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.NamedAttributeNode;
import javax.persistence.NamedEntityGraph;
import javax.persistence.OneToOne;

import com.latticeengines.common.exposed.util.AvroUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;

import io.swagger.annotations.ApiModel;

@Entity
@javax.persistence.Table(name = "AI_MODEL")
@JsonIgnoreProperties(ignoreUnknown = true)
@OnDelete(action = OnDeleteAction.CASCADE)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@NamedEntityGraph(name = "AIModel.details", attributeNodes = {@NamedAttributeNode("ratingEngine"), @NamedAttributeNode("trainingSegment")})
@ApiModel("Represents AIModel JSON Object")
public class AIModel extends RatingModel {

	public static final String AI_MODEL_PREFIX = "ai";
    public static final String AI_MODEL_FORMAT = "%s__%s";
    
    @JsonIgnore
    @Column(name = "TARGET_PRODUCTS", length=10000)
    @Type(type = "text")
    private String targetProducts;
    
    @JsonProperty("modelingMethod")
    @Column(name = "MODELING_METHOD")
    @Enumerated(EnumType.STRING)
    private ModelingMethod modelingMethod;
    
    @JsonProperty("workflowType")
    @Column(name = "WORKFLOW_TYPE")
    @Enumerated(EnumType.STRING)
    private ModelWorkflowType workflowType;
    
    @JsonIgnore
    @Column(name = "TRAINING_PRODUCTS", length=10000)
    @Type(type = "text")
    private String trainingProducts;
    
    @JsonProperty("modelingJobId")
    @Column(name = "MODELING_JOBID")
    private String modelingJobId;
    
    @JsonProperty("targetCustomerSet")
    @Column(name = "TARGET_CUSTOMER_SET")
    private String targetCustomerSet;
    
    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TRAINING_SEGMENT_ID")
    @JsonProperty("trainingSegment")
    private MetadataSegment trainingSegment;
    
    @OneToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_MODEL_SUMMARY_ID")
    @JsonProperty("modelSummary")
    private ModelSummary modelSummary;

    @JsonIgnore
    @Column(name = "MODELING_CONFIG_FILTERS", length=10000)
    @Type(type = "text")
    private String modelingConfigFilters;
    
    @JsonProperty("targetProducts")
	public List<String> getTargetProducts() {
    		List<String> productList = null;
        if (StringUtils.isNotBlank(this.targetProducts)) {
            List<?> attrListIntermediate = JsonUtils.deserialize(this.targetProducts, List.class);
            productList = JsonUtils.convertList(attrListIntermediate, String.class);
        }
        return productList;
	}

    @JsonProperty("targetProducts")
	public void setTargetProducts(List<String> targetProducts) {
		this.targetProducts = JsonUtils.serialize(targetProducts);
	}

    public static String generateIdStr() {
		String uuid = AvroUtils.getAvroFriendlyString(UuidUtils.shortenUuid(UUID.randomUUID()));
		return String.format(AI_MODEL_FORMAT, AI_MODEL_PREFIX, uuid);
    }

	public ModelingMethod getModelingMethod() {
		return modelingMethod;
	}

	public void setModelingMethod(ModelingMethod modelingMethod) {
		this.modelingMethod = modelingMethod;
	}

	public ModelWorkflowType getWorkflowType() {
		return workflowType;
	}

	public void setWorkflowType(ModelWorkflowType workflowType) {
		this.workflowType = workflowType;
	}

	@JsonProperty("trainingProducts")
	public List<String> getTrainingProducts() {
		List<String> trainingProductList = null;
        if (StringUtils.isNotBlank(this.trainingProducts)) {
            List<?> attrListIntermediate = JsonUtils.deserialize(this.trainingProducts, List.class);
            trainingProductList = JsonUtils.convertList(attrListIntermediate, String.class);
        }
		return trainingProductList;
	}
	
	@JsonProperty("trainingProducts")
	public void setTrainingProducts(List<String> trainingProducts) {
		this.trainingProducts = JsonUtils.serialize(trainingProducts);;
	}

	public MetadataSegment getTrainingSegment() {
		return trainingSegment;
	}

	public void setTrainingSegment(MetadataSegment trainingSegment) {
		this.trainingSegment = trainingSegment;
	}

    @JsonIgnore
    public ApplicationId getModelingJobId() {
    		if(StringUtils.isNotBlank(modelingJobId))
    			return ConverterUtils.toApplicationId(modelingJobId);
    		return null;
	}
    
	public void setModelingJobId(String modelingJobId) {
		this.modelingJobId = modelingJobId;
	}

	@JsonProperty("modelingConfigFilters")
	public Map<ModelingConfig, ModelingConfigFilter> getModelingConfigFilters() {
		Map<ModelingConfig, ModelingConfigFilter> filters = null;
        if (StringUtils.isNotBlank(this.modelingConfigFilters)) {
            Map<?, ?> attrListIntermediate = JsonUtils.deserialize(this.modelingConfigFilters, Map.class);
            filters = JsonUtils.convertMap(attrListIntermediate, ModelingConfig.class, ModelingConfigFilter.class);
        }
        return filters;
	}

	@JsonProperty("modelingConfigFilters")
	public void setModelingConfigFilters(Map<ModelingConfig, ModelingConfigFilter> filters) {
		this.modelingConfigFilters = JsonUtils.serialize(filters);
	}

	public String getTargetCustomerSet() {
		return targetCustomerSet;
	}

	public void setTargetCustomerSet(String targetCustomerSet) {
		this.targetCustomerSet = targetCustomerSet;
	}
	
    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

	public ModelSummary getModelSummary() {
		return modelSummary;
	}

	public void setModelSummary(ModelSummary modelSummary) {
		this.modelSummary = modelSummary;
	}
}
