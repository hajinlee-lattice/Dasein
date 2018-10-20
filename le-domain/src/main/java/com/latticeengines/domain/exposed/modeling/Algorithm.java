package com.latticeengines.domain.exposed.modeling;

import java.util.Properties;

import javax.persistence.Embeddable;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.modeling.algorithm.DecisionTreeAlgorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.LogisticRegressionAlgorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.PMMLAlgorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.RandomForestAlgorithm;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = LogisticRegressionAlgorithm.class, name = "logisticRegressionAlgorithm"),
        @JsonSubTypes.Type(value = DecisionTreeAlgorithm.class, name = "decisionTreeAlgorithm"),
        @JsonSubTypes.Type(value = RandomForestAlgorithm.class, name = "randomForestAlgorithm"),
        @JsonSubTypes.Type(value = PMMLAlgorithm.class, name = "pmmlAlgorithm") })
@Embeddable
public interface Algorithm extends HasName, HasPid {

    @Override
    String getName();

    String getScript();

    void setScript(String script);

    String getContainerProperties();

    void setContainerProperties(String containerProperties);

    String getAlgorithmProperties();

    void setAlgorithmProperties(String algorithmProperties);

    Properties getAlgorithmProps();

    void setAlgorithmProps(Properties props);

    Properties getContainerProps();

    int getPriority();

    void setPriority(int priority);

    String getSampleName();

    void setSampleName(String sampleName);

    ModelDefinition getModelDefinition();

    void setModelDefinition(ModelDefinition modelDefinition);

    String getPipelineScript();

    void setPipelineScript(String pipelineScript);

    String getPipelineLibScript();

    void setPipelineLibScript(String pipelineLibScript);

    String getMapperSize();

    void setMapperSize(String mapperSize);

    String getPipelineProperties();

    void setPipelineProperties(String pipelineProperties);

    Properties getPipelineProps();

    String getPipelineDriver();

    void setPipelineDriver(String pipelineDriver);

    void resetAlgorithmProperties();

    boolean hasDataDiagnostics();

}
