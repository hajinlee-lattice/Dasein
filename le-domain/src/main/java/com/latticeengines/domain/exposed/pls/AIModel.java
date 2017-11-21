package com.latticeengines.domain.exposed.pls;

import java.util.List;
import java.util.UUID;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.NamedAttributeNode;
import javax.persistence.NamedEntityGraph;
import javax.persistence.Transient;

import org.apache.commons.lang3.StringUtils;
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

@Entity
@javax.persistence.Table(name = "AI_MODEL")
@JsonIgnoreProperties(ignoreUnknown = true)
@OnDelete(action = OnDeleteAction.CASCADE)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@NamedEntityGraph(name = "AIModel.details", attributeNodes = {@NamedAttributeNode("ratingEngine"), @NamedAttributeNode("trainingSegment")})
public class AIModel extends RatingModel {

    public static final String AI_MODEL_PREFIX = "ai_model";
    public static final String AI_MODEL_FORMAT = "%s__%s";

    @JsonIgnore
    @Column(name = "TARGET_PRODUCTS", length=10000)
    @Type(type = "text")
    private String targetProducts;
    
    @Column(name = "MODELING_METHOD")
    @Enumerated(EnumType.STRING)
    private ModelingMethod modelingMethod;
    
    @Column(name = "WORKFLOW_TYPE")
    @Enumerated(EnumType.STRING)
    private ModelWorkflowType workflowType;
    
    @JsonIgnore
    @Column(name = "TRAINING_PRODUCTS", length=10000)
    @Type(type = "text")
    private String trainingProducts;
    
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_TRAINING_SEGMENT_ID")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private MetadataSegment trainingSegment;
    
    // TODO in M14, we do not support AImodel
    // @OneToMany(cascade = { CascadeType.ALL }, mappedBy = "aImodel")
    @Transient
    @JsonProperty("modelSummary")
    private ModelSummary modelSummary;


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
		this.targetProducts = JsonUtils.serialize(targetProducts);;
	}
	
    public void setModel(ModelSummary modelSummary) {
        this.modelSummary = modelSummary;
    }

    public ModelSummary getModel() {
        return this.modelSummary;
    }

    public static String generateIdStr() {
        return String.format(AI_MODEL_FORMAT, AI_MODEL_PREFIX, UuidUtils.shortenUuid(UUID.randomUUID()));
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

}
