package com.latticeengines.domain.exposed.dataplatform.dlorchestration;


public enum ModelCommandStep {
    LOAD_DATA("Load data"),
    GENERATE_SAMPLES("Generate samples"),
    PROFILE_DATA("Profile data"),
    SUBMIT_MODELS("Submit models"),
    FINISH("Finished");
    
    private String description;
    
    private ModelCommandStep(String description) {
        this.description = description;
    }
    
    public String getDescription() {
        return description;
    }
    
    public ModelCommandStep getNextStep() {
        return ModelCommandStep.values()[ordinal()+1];
    }
}
