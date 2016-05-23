package com.latticeengines.scheduler.exposed;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LedpQueueAssigner {

    public static final String PRIORITY = "Priority";
    public static final Log log = LogFactory.getLog(LedpQueueAssigner.class);

    private static final String SCORING_QUEUE_NAME = "Scoring";
    private static final String MODELING_QUEUE_NAME = "Modeling";
    private static final String PROPDATA_QUEUE_NAME = "PropData";
    private static final String WORKFLOW_QUEUE_NAME = "Workflow";
    private static final String DATAFLOW_QUEUE_NAME = "Dataflow";
    private static final String EAI_QUEUE_NAME = "Eai";
    private static final String DEFAULT_QUEUE_NAME = "default";

    public static String getScoringQueueNameForSubmission() {
        return SCORING_QUEUE_NAME;
    }

    public static String getModelingQueueNameForSubmission() {
        return MODELING_QUEUE_NAME;
    }

    public static String getPropDataQueueNameForSubmission() {
        return PROPDATA_QUEUE_NAME;
    }

    public static String getWorkflowQueueNameForSubmission() {
        return WORKFLOW_QUEUE_NAME;
    }

    public static String getDataflowQueueNameForSubmission() {
        return DATAFLOW_QUEUE_NAME;
    }

    public static String getEaiQueueNameForSubmission() {
        return EAI_QUEUE_NAME;
    }

    public static String getDefaultQueueNameForSubmission() {
        return DEFAULT_QUEUE_NAME;
    }

    public static String overwriteQueueAssignment(String queue, String queueScheme) {
        String translatedQueue = queue;
        if (queue == null) {
            return queue;
        }
        if (queueScheme.equalsIgnoreCase("default")) {
            translatedQueue = LedpQueueAssigner.getDefaultQueueNameForSubmission();
        } else if (queueScheme.equalsIgnoreCase("legacy")) {
            if (queue.equals(LedpQueueAssigner.getWorkflowQueueNameForSubmission()) ||
                    queue.equals(LedpQueueAssigner.getDataflowQueueNameForSubmission()) ||
                    queue.equals(LedpQueueAssigner.getEaiQueueNameForSubmission())) {
                translatedQueue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
            }
        }
        if (!translatedQueue.equals(queue)) {
            log.info("Overwite queue " + queue + " to " + translatedQueue);
        }

        return translatedQueue;
    }

}
