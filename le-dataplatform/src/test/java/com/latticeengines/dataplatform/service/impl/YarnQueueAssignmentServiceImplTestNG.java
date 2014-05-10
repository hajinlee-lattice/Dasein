package com.latticeengines.dataplatform.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;

public class YarnQueueAssignmentServiceImplTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private YarnQueueAssignmentServiceImpl yarnQueueAssignmentService;

    @Override
    protected boolean doYarnClusterSetup() {
        return false;
    }
    
    @Test(groups = "functional")
    public void testNewlyAssignedAllEqualUtilizedMRQueue() throws Exception {       
        final String customer = "Nobody";
        final String requestedParentQueue = "MapReduce";
        String assignedQueueName = yarnQueueAssignmentService.getAssignedQueue(customer, requestedParentQueue);
        assertTrue(assignedQueueName.contains("root.Priority0.MapReduce"));                 
    }     
    
    @Test(groups = "functional")
    public void testNewlyAssignedAllEqualUtilizedNonMRQueue() throws Exception {       
        final String customer = "Nobody";
        final String requestedParentQueue = "Priority0";
        String assignedQueueName = yarnQueueAssignmentService.getAssignedQueue(customer, requestedParentQueue);
        assertTrue(assignedQueueName.contains("root.Priority0"));               
    }       
    
    @Test(groups = "functional")
    public void testNewlyAssignedRequestedParentQueueDoesNotExist() throws Exception {       
        final String customer = "Nobody";
        final String requestedParentQueue = "ThisParentQueueDoesNotExist";
        
        try {            
            yarnQueueAssignmentService.getAssignedQueue(customer, requestedParentQueue);
        } catch (LedpException e) {
            assertEquals(LedpException.buildMessage(LedpCode.LEDP_12001, new String[] { requestedParentQueue }), e.getMessage());           
        }
    }    
}
