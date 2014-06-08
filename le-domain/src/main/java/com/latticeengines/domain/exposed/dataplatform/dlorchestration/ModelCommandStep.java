package com.latticeengines.domain.exposed.dataplatform.dlorchestration;


public enum ModelCommandStep {
    LOAD_DATA,
    GENERATE_SAMPLES,
    SUBMIT_MODELS,
    SEND_JSON;
    
//   public static final EnumSet<ModelCommandStep> YARN_STATES = EnumSet.range(LOAD_CUSTOMER_DATA, SUBMIT_MODELS);
    
    public ModelCommandStep getNextStep() {
        return ModelCommandStep.values()[ordinal()+1];
    }
}
