package com.latticeengines.dataplatform.service.impl;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.StringReader;
import java.net.URI;
import java.net.URL;

import javax.xml.bind.JAXBContext;

import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.mockito.Mock;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.dataplatform.service.YarnQueueAssignmentService;

public class YarnQueueAssignmentServiceImplUnitTestNG {

    @Mock
    private YarnService yarnService;
    
    private YarnQueueAssignmentService yarnQueueAssignmentService;

    @BeforeClass(groups = "unit")
    public void beforeClass() throws Exception {
        initMocks(this);
        yarnQueueAssignmentService = setupAssignmentService();
    }
    
    private YarnQueueAssignmentService setupAssignmentService() throws Exception {
        SchedulerTypeInfo schedulerInfo = unmarshalSchedulerTypeInfo();
        AppsInfo appsInfo = unmarshalAppsInfo();
        
        when(yarnService.getSchedulerInfo()).thenReturn(schedulerInfo);
        when(yarnService.getApplications(YarnQueueAssignmentServiceImpl.APP_STATES_ACCEPTED_RUNNING)).thenReturn(appsInfo);
        YarnQueueAssignmentServiceImpl assignmentService = new YarnQueueAssignmentServiceImpl();
        assignmentService.setYarnService(yarnService);
        
        return assignmentService;
    }
    
    private static String getStringFromFile(String path) throws Exception {
        URL url = ClassLoader.getSystemResource(path);
        String filePath = "file:" + url.getFile();   
        return Files.toString(new File(new URI(filePath)), Charsets.UTF_8);
    }
    
    private static SchedulerTypeInfo unmarshalSchedulerTypeInfo() throws Exception {
        String mockFilePath = "com/latticeengines/dataplatform/service/impl/yqasMockSchedulerInfo.xml";
                
        return (SchedulerTypeInfo) JAXBContext.newInstance(SchedulerTypeInfo.class)
                                 .createUnmarshaller()
                                 .unmarshal(new StringReader(getStringFromFile(mockFilePath)));
    }    
    
    private static AppsInfo unmarshalAppsInfo() throws Exception {
        String mockFilePath = "com/latticeengines/dataplatform/service/impl/yqasMockAppsInfo.xml";
        
        return (AppsInfo) JAXBContext.newInstance(AppsInfo.class)
                                 .createUnmarshaller()
                                 .unmarshal(new StringReader(getStringFromFile(mockFilePath)));
    }   
      
    @Test(groups = "unit")
    public void testStickyP0NonMRQueue() throws Exception {       
        final String customer = "Dell";
        final String requestedParentQueue = "Priority0";
        
        assertEquals("root.Priority0.C", yarnQueueAssignmentService.getAssignedQueue(customer, requestedParentQueue));
    }    
    
    @Test(groups = "unit")
    public void testStickyP1NonMRQueue() throws Exception {       
        final String customer = "Dell";
        final String requestedParentQueue = "Priority1";
        
        assertEquals("root.Priority1.B", yarnQueueAssignmentService.getAssignedQueue(customer, requestedParentQueue));
    }   
    
    @Test(groups = "unit")
    public void testStickyP0MRQueue() throws Exception {       
        final String customer = "Dell";
        final String requestedParentQueue = "MapReduce";
        
        assertEquals("root.Priority0.MapReduce.A", yarnQueueAssignmentService.getAssignedQueue(customer, requestedParentQueue));
    }     
   
    @Test(groups = "unit")
    public void testNewlyAssignedGetLeastUtilizedMRQueue() throws Exception {       
        final String customer = "Nobody";
        final String requestedParentQueue = "MapReduce";
        
        assertEquals("root.Priority0.MapReduce.B", yarnQueueAssignmentService.getAssignedQueue(customer, requestedParentQueue));
    }        
    
    @Test(groups = "unit")
    public void testNewlyAssignedGetLeastUtilizedNonMRQueue() throws Exception {       
        final String customer = "Nobody";
        final String requestedParentQueue = "Priority0";
        
        assertEquals("root.Priority0.D", yarnQueueAssignmentService.getAssignedQueue(customer, requestedParentQueue));
    }     
    
    @Test(groups = "unit")
    public void testNewlyAssignedAllEqualUtilizedNonMRQueue() throws Exception {       
        final String customer = "Nobody";
        final String requestedParentQueue = "Priority1";
        
        assertEquals("root.Priority1.A", yarnQueueAssignmentService.getAssignedQueue(customer, requestedParentQueue));
    }    
    
    @Test(groups = "unit")
    public void testNewlyAssignedRequestedParentQueueDoesNotExist() throws Exception {       
        final String customer = "Nobody";
        final String requestedParentQueue = "ThisParentQueueDoesNotExist";
        
        try {            
            yarnQueueAssignmentService.getAssignedQueue(customer, requestedParentQueue);
        } catch (LedpException e) {
            assertEquals(LedpException.buildMessage(LedpCode.LEDP_12001, new String[] { requestedParentQueue }), e.getMessage());           
        }
    }
     
    @Test(groups = "unit")
    public void testNewlyAssignedNoLeafQueueForParentQueue() throws Exception {       
        final String customer = "Nobody";
        final String requestedParentQueue = "PriorityNoLeaves";
        
        try {            
            yarnQueueAssignmentService.getAssignedQueue(customer, requestedParentQueue);
        } catch (LedpException e) {
            assertEquals(LedpException.buildMessage(LedpCode.LEDP_12002, new String[] { requestedParentQueue }), e.getMessage());           
        }
        
        assertEquals(1, 2);
    }        
}
