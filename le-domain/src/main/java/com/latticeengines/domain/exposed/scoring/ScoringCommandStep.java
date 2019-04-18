package com.latticeengines.domain.exposed.scoring;

public enum ScoringCommandStep {

    LOAD_DATA("Load data"), SCORE_DATA("Score data"), EXPORT_DATA("Export data"), FINISH(
            "Finished");

    private String description;

    ScoringCommandStep(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    public ScoringCommandStep getNextStep() {
        return ScoringCommandStep.values()[ordinal() + 1];
    }
}
