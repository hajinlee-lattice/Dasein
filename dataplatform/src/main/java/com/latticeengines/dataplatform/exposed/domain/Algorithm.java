package com.latticeengines.dataplatform.exposed.domain;

import java.util.Properties;

public interface Algorithm extends HasName {

    String getName();

    String getScript();

    String getContainerProperties();

    void setContainerProperties(String containerProperties);

    String getAlgorithmProperties();
    
    void setAlgorithmProperties(String algorithmProperties);

    Properties getAlgorithmProps();

    Properties getContainerProps();

    int getPriority();
    
    void setPriority(int priority);
    
    String getSampleName();
    
    void setSampleName(String sampleName);

}
