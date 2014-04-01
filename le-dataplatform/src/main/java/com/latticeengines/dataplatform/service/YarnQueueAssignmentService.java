package com.latticeengines.dataplatform.service;


public interface YarnQueueAssignmentService  {

    public enum AssignmentPolicy { 
        DEFAULT, 
        // by active + pending
        SHORTESTQUEUE, 
        // round robin
        ROUNDROBIN, 
        // random
        RANDOM, 
        // by pending
        SHORTESTPENDINGQUEUE,
        // by total active + pending resources (memory)
        SHORTESTQUEUEBYRESOURCES,
        // sticky to minimize data movement
        DATALOCALITY,
        // sticky shortest queue
        STICKYSHORTESTQUEUE;

    }

    String useQueue(String assignmentToken, AssignmentPolicy policy); 
    
    String useQueue(String assignmentToken, AssignmentPolicy policy, Boolean refreshQueueState); 

}
