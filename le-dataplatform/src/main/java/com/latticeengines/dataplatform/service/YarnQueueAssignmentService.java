package com.latticeengines.dataplatform.service;

public interface YarnQueueAssignmentService {

    /**
     * The assignment policy is sticky or least utilized. If a customer has an
     * existing live application associated to a leaf queue, then that queue is
     * returned. Otherwise, the least utilized leaf queue is returned if at
     * least one is available.
     * 
     * @param customer
     * @param parentQueue
     * @return
     */
    String getAssignedQueue(String customer, String parentQueue);

}
