package com.latticeengines.dataplatform.service.impl;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

import org.apache.commons.io.FileUtils;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;

import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.dataplatform.service.YarnQueueAssignmentService.AssignmentPolicy;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerLeafQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.QueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.conf.Configuration;

public class YarnQueueAssignmentServiceImplUnitTestNG {

    @Mock
    YarnService yarnService;
    
    @BeforeClass(groups = "unit")
    public void beforeClass() {
        MockitoAnnotations.initMocks(this);
    }

    private void setupFairSchedulerConfig() throws Exception
    {
        PrintWriter out = new PrintWriter(new FileWriter("target/test-classes/fair-scheduler.xml"));
        out.println("<?xml version=\"1.0\"?>");
        out.println("<allocations>");
        out.println("   <queue name=\"PlayMaker\">");
        out.println("       <weight>5</weight>");
        out.println("       <minResources>2048 mb,2 vcores</minResources>");
        out.println("       <schedulingPolicy>fair</schedulingPolicy>");
        out.println("       <minSharePreemptionTimeout>3</minSharePreemptionTimeout>");
        out.println("       <queue name=\"1\">");
        out.println("           <weight>5</weight>");
        out.println("           <minResources>4096 mb,4 vcores</minResources>");
        out.println("           <fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
        out.println("           <schedulingPolicy>fifo</schedulingPolicy>");
        out.println("       </queue>");
        out.println("       <queue name=\"2\">");
        out.println("           <weight>5</weight>");
        out.println("           <minResources>4096 mb,4 vcores</minResources>");
        out.println("           <fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
        out.println("           <schedulingPolicy>fifo</schedulingPolicy>");
        out.println("       </queue>");
        out.println("       <queue name=\"3\">");
        out.println("           <weight>20</weight>");
        out.println("           <minResources>8096 mb,8 vcores</minResources>");
        out.println("           <fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
        out.println("           <schedulingPolicy>fifo</schedulingPolicy>");
        out.println("       </queue>");
        out.println("       <queue name=\"4\">");
        out.println("           <weight>20</weight>");
        out.println("           <minResources>8096 mb,8 vcores</minResources>");
        out.println("           <fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
        out.println("           <schedulingPolicy>fifo</schedulingPolicy>");
        out.println("       </queue>");
        out.println("       <queue name=\"5\">");
        out.println("           <weight>20</weight>");
        out.println("           <minResources>8096 mb,8 vcores</minResources>");
        out.println("           <fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
        out.println("           <schedulingPolicy>fifo</schedulingPolicy>");
        out.println("       </queue>");
        out.println("       <queue name=\"6\">");
        out.println("           <weight>20</weight>");
        out.println("           <minResources>8096 mb,8 vcores</minResources>");
        out.println("           <fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
        out.println("           <schedulingPolicy>fifo</schedulingPolicy>");
        out.println("       </queue>");
        out.println("       <queue name=\"7\">");
        out.println("           <weight>20</weight>");
        out.println("           <minResources>8096 mb,8 vcores</minResources>");
        out.println("           <fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
        out.println("           <schedulingPolicy>fifo</schedulingPolicy>");
        out.println("       </queue>");
        out.println("       <queue name=\"8\">");
        out.println("           <weight>20</weight>");
        out.println("           <minResources>8096 mb,8 vcores</minResources>");
        out.println("           <fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
        out.println("           <schedulingPolicy>fifo</schedulingPolicy>");
        out.println("       </queue>");
        out.println("       <queue name=\"12\">");
        out.println("           <weight>20</weight>");
        out.println("           <minResources>8096 mb,8 vcores</minResources>");
        out.println("           <fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
        out.println("           <schedulingPolicy>fifo</schedulingPolicy>");
        out.println("       </queue>");
        out.println("       <queue name=\"15\">");
        out.println("           <weight>20</weight>");
        out.println("           <minResources>8096 mb,8 vcores</minResources>");
        out.println("           <fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
        out.println("           <schedulingPolicy>fifo</schedulingPolicy>");
        out.println("       </queue>");
        out.println("   </queue>");
        out.println("   <queue name=\"FastLane\">");
        out.println("       <minResources>2048 mb,2 vcores</minResources>");
        out.println("       <schedulingPolicy>fair</schedulingPolicy>");
        out.println("       <minSharePreemptionTimeout>3</minSharePreemptionTimeout>");
        out.println("   </queue>");
        out.println("   <defaultMinSharePreemptionTimeout>3</defaultMinSharePreemptionTimeout>");
        out.println("   <fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
        out.println("</allocations>");
        out.close();
    }
    
    @Test(groups = "unit")
    public void testNoQueues() {
        FairSchedulerInfo fairSchedulerInfoToReturn = new FairSchedulerInfo();
        Mockito.when(yarnService.getSchedulerInfo()).thenReturn(new SchedulerTypeInfo(fairSchedulerInfoToReturn));
        AppsInfo appsInfoToReturn = new AppsInfo();
        Mockito.when(yarnService.getApplications("states=running,accepted")).thenReturn(appsInfoToReturn);
        YarnQueueAssignmentServiceImpl service = new YarnQueueAssignmentServiceImpl();
        service.setYarnService(yarnService);
        try {
            String queueName = service.useQueue("Dell", AssignmentPolicy.STICKYSHORTESTQUEUE, true);
            System.out.println(queueName);
        }
        catch (LedpException e)
        {
            Assert.assertEquals(e.getMessage(), LedpCode.LEDP_12002.getMessage());
            return;
        }
    }

    @Test(groups = "unit")
    public void testSingleQueueNoCurrentAssignmentZeroDepth() throws Exception {
        setupFairSchedulerConfig();
        Configuration conf = new Configuration();
        FairScheduler fairScheduler = new FairScheduler();
        fairScheduler.reinitialize(conf, null);
        FairSchedulerInfo fairSchedulerInfoToReturn = new FairSchedulerInfo(fairScheduler);
        Mockito.when(yarnService.getSchedulerInfo()).thenReturn(new SchedulerTypeInfo(fairSchedulerInfoToReturn));
        AppsInfo appsInfoToReturn = new AppsInfo();
        Mockito.when(yarnService.getApplications("states=running,accepted")).thenReturn(appsInfoToReturn);
        YarnQueueAssignmentServiceImpl service = new YarnQueueAssignmentServiceImpl();
        service.setYarnService(yarnService);
        String queueName = service.useQueue("Dell", AssignmentPolicy.STICKYSHORTESTQUEUE, true);
        Assert.assertEquals(queueName, "root.PlayMaker.4");
        queueName = service.useQueue("Microsoft", AssignmentPolicy.STICKYSHORTESTQUEUE, true);
        Assert.assertEquals(queueName, "root.PlayMaker.5");
        queueName = service.useQueue("CDW", AssignmentPolicy.STICKYSHORTESTQUEUE, true);
        Assert.assertEquals(queueName, "root.PlayMaker.1");
        queueName = service.useQueue("OfficeMax", AssignmentPolicy.STICKYSHORTESTQUEUE, true);
        Assert.assertEquals(queueName, "root.PlayMaker.12");
        queueName = service.useQueue("Charles Schwab", AssignmentPolicy.STICKYSHORTESTQUEUE, true);
        Assert.assertEquals(queueName, "root.PlayMaker.1");
        queueName = service.useQueue("VMWare Inc", AssignmentPolicy.STICKYSHORTESTQUEUE, true);
        Assert.assertEquals(queueName, "root.PlayMaker.3");
    }
  
    @Test(groups = "unit")
    public void testSingleQueueUseCurrentAssignment() throws Exception {
        setupFairSchedulerConfig();
        Configuration conf = new Configuration();
        FairScheduler fairScheduler = new FairScheduler();
        fairScheduler.reinitialize(conf, null);
        FairSchedulerInfo fairSchedulerInfoToReturn = new FairSchedulerInfo(fairScheduler);
        Mockito.when(yarnService.getSchedulerInfo()).thenReturn(new SchedulerTypeInfo(fairSchedulerInfoToReturn));
        AppsInfo appsInfoToReturn = new AppsInfo();
        AppInfo app1 = new AppInfo(new RMAppImpl(new ApplicationIdPBImpl(ApplicationIdProto.getDefaultInstance()), 
                                                 new RMContextImpl(new AsyncDispatcher(), null, null, null, null, null, null, null, null), 
                                                 conf, 
                                                 "Dell.SellPrinters.LogisticRegression.Small", 
                                                 "John", 
                                                 "root.PlayMaker.10", 
                                                 new ApplicationSubmissionContextPBImpl(), 
                                                 fairScheduler, 
                                                 null,
                                                 0, 
                                                 null ), 
                                     true);
        appsInfoToReturn.add(app1);
        AppInfo app2 = new AppInfo(new RMAppImpl(new ApplicationIdPBImpl(ApplicationIdProto.getDefaultInstance()), 
                                                 new RMContextImpl(new AsyncDispatcher(), null, null, null, null, null, null, null, null), 
                                                 conf, 
                                                 "OfficeMax.SellPaper.RandomForest.Large", 
                                                 "Jane", 
                                                 "root.PlayMaker.4", 
                                                 new ApplicationSubmissionContextPBImpl(), 
                                                 fairScheduler, 
                                                 null,
                                                 0, 
                                                 null ), 
                                     true);
        appsInfoToReturn.add(app2);
        Mockito.when(yarnService.getApplications("states=running,accepted")).thenReturn(appsInfoToReturn);
        YarnQueueAssignmentServiceImpl service = new YarnQueueAssignmentServiceImpl();
        service.setYarnService(yarnService);
        String queueName = service.useQueue("Dell", AssignmentPolicy.STICKYSHORTESTQUEUE, true);
        Assert.assertEquals(queueName, "root.PlayMaker.10");
        queueName = service.useQueue("OfficeMax", AssignmentPolicy.STICKYSHORTESTQUEUE, true);
        Assert.assertEquals(queueName, "root.PlayMaker.4");
    }
  
}
