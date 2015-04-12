package com.latticeengines.domain.exposed.scoring;

public enum ScoringCommandStep {

    VALIDATE_DATA("Validate data"),
    LOAD_DATA("Load data"),
//    SCORE_DATA("Score data"),
//    EXPORT_DATA("Export data"),
    OUTPUT_COMMAND_RESULTS("Output command results"),
    FINISH("Finished");

    private String description;

    private ScoringCommandStep(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    public ScoringCommandStep getNextStep() {
        return ScoringCommandStep.values()[ordinal()+1];
    }
}
