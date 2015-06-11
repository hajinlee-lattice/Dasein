package com.latticeengines.scheduler.exposed;


public class LedpQueueAssigner {

    public static final String PRIORITY = "Priority";

    private static final String SCORING_QUEUE_NAME = "Scoring";
    private static final String MODELING_QUEUE_NAME = "Modeling";
    private static final String PROPDATA_QUEUE_NAME = "PropData";

    public static String getScoringQueueNameForSubmission() {
        return SCORING_QUEUE_NAME;
    }

    public static String getModelingQueueNameForSubmission() {
        return MODELING_QUEUE_NAME;
    }

    public static String getPropDataQueueNameForSubmission() {
        return PROPDATA_QUEUE_NAME;
    }
}
