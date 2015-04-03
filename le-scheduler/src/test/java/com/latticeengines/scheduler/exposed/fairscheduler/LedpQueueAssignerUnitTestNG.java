package com.latticeengines.scheduler.exposed.fairscheduler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.QueueManager;
import org.mockito.Mock;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.scheduler.exposed.fairscheduler.LedpQueueAssigner;

public class LedpQueueAssignerUnitTestNG {

    private static final String PRIORITY_NO_LEAVES = "PriorityNoLeaves";

    private static final String DELL_APPLICATION_ID = "application_1400266750643_0092";

    private LedpQueueAssigner queueAssigner;

    @Mock
    private RMApp rmAppDell;

    @Mock
    private RMApp rmAppNobody;

    @Mock
    private QueueManager queueManager;

    @BeforeClass(groups = "unit")
    public void beforeClass() throws Exception {
        initMocks(this);

        queueAssigner = new LedpQueueAssigner();
        Map<String, String> appIdToCustomer = new HashMap<String, String>();
        appIdToCustomer.put(DELL_APPLICATION_ID, "Dell");
        ReflectionTestUtils.setField(queueAssigner, "appIdToCustomer", appIdToCustomer);

        // For an easier view, the mocked QueueManager reflects the queue
        // structure captured in yqasMockSchedulerInfo.xml.
        queueManager = mock(QueueManager.class);

        FSQueue p0MRQ = generateP0MapReduceQueue();
        FSQueue p0Q = generateP0Queue(p0MRQ);
        FSQueue p1Q = generateP1Queue();
        FSQueue emptyQ = generateQueueWithEmptyLeaves();

        when(queueManager.getQueue("Priority0")).thenReturn(p0Q);
        when(queueManager.getQueue("Priority0.MapReduce")).thenReturn(p0MRQ);
        when(queueManager.getQueue("Priority1")).thenReturn(p1Q);
        when(queueManager.getQueue(PRIORITY_NO_LEAVES)).thenReturn(emptyQ);

        ApplicationId dellApplicationId = mock(ApplicationId.class);
        when(rmAppDell.getApplicationId()).thenReturn(dellApplicationId);
        when(dellApplicationId.toString()).thenReturn(DELL_APPLICATION_ID);
        when(rmAppDell.getName()).thenReturn("Dell~pythonClient~2014-05-16T15:01:01.398-04:00");

        ApplicationId nobodyApplicationId = mock(ApplicationId.class);
        when(rmAppNobody.getApplicationId()).thenReturn(nobodyApplicationId);
        when(nobodyApplicationId.toString()).thenReturn("application_1400266750643_0093");
        when(rmAppNobody.getName()).thenReturn("Nobody~pythonClient~2014-05-16T15:01:01.398-04:00");
    }

    private FSQueue generateP0Queue(FSQueue p0MRQ) {
        FSQueue p0Q = mock(FSQueue.class);
        Collection<FSQueue> p0childQueues = new ArrayList<FSQueue>();
        p0childQueues.add(p0MRQ);

        Collection<FSAppAttempt> apps;

        FSLeafQueue p0_0 = mock(FSLeafQueue.class);
        apps = generateAppSchedCollectionOfSize(10);
        when(p0_0.getRunnableAppSchedulables()).thenReturn(apps);
        when(p0_0.getQueueName()).thenReturn("root.Priority0.0");
        p0childQueues.add(p0_0);

        FSLeafQueue p0_1 = mock(FSLeafQueue.class);
        apps = generateAppSchedCollectionOfSize(8);
        when(p0_1.getRunnableAppSchedulables()).thenReturn(apps);
        when(p0_1.getQueueName()).thenReturn("root.Priority0.1");
        p0childQueues.add(p0_1);

        FSLeafQueue p0_2 = mock(FSLeafQueue.class);
        apps = generateAppSchedCollectionOfSize(6, DELL_APPLICATION_ID);
        when(p0_2.getRunnableAppSchedulables()).thenReturn(apps);
        when(p0_2.getQueueName()).thenReturn("root.Priority0.2");
        p0childQueues.add(p0_2);

        FSLeafQueue p0_3 = mock(FSLeafQueue.class);
        apps = generateAppSchedCollectionOfSize(2);
        when(p0_3.getRunnableAppSchedulables()).thenReturn(apps);
        when(p0_3.getQueueName()).thenReturn("root.Priority0.3");
        p0childQueues.add(p0_3);

        FSLeafQueue p0_4 = mock(FSLeafQueue.class);
        apps = generateAppSchedCollectionOfSize(4);
        when(p0_4.getRunnableAppSchedulables()).thenReturn(apps);
        when(p0_4.getQueueName()).thenReturn("root.Priority0.4");
        p0childQueues.add(p0_4);

        when(p0Q.getChildQueues()).thenReturn((List<FSQueue>) p0childQueues);

        return p0Q;
    }

    private FSQueue generateP0MapReduceQueue() {
        FSQueue p0MRQ = mock(FSQueue.class);
        Collection<FSQueue> p0MRchildQueues = new ArrayList<FSQueue>();
        when(p0MRQ.getChildQueues()).thenReturn((List<FSQueue>) p0MRchildQueues);

        Collection<FSAppAttempt> apps;

        FSLeafQueue p0MR_0 = mock(FSLeafQueue.class);
        apps = generateAppSchedCollectionOfSize(30, DELL_APPLICATION_ID);
        when(p0MR_0.getRunnableAppSchedulables()).thenReturn(apps);
        when(p0MR_0.getQueueName()).thenReturn("root.Priority0.MapReduce.0");
        p0MRchildQueues.add(p0MR_0);

        FSLeafQueue p0MR_1 = mock(FSLeafQueue.class);
        apps = generateAppSchedCollectionOfSize(0);
        when(p0MR_1.getRunnableAppSchedulables()).thenReturn(apps);
        when(p0MR_1.getQueueName()).thenReturn("root.Priority0.MapReduce.1");
        p0MRchildQueues.add(p0MR_1);

        FSLeafQueue p0MR_2 = mock(FSLeafQueue.class);
        apps = generateAppSchedCollectionOfSize(30);
        when(p0MR_2.getRunnableAppSchedulables()).thenReturn(apps);
        when(p0MR_2.getQueueName()).thenReturn("root.Priority0.MapReduce.2");
        p0MRchildQueues.add(p0MR_2);

        FSLeafQueue p0MR_3 = mock(FSLeafQueue.class);
        apps = generateAppSchedCollectionOfSize(30);
        when(p0MR_3.getRunnableAppSchedulables()).thenReturn(apps);
        when(p0MR_3.getQueueName()).thenReturn("root.Priority0.MapReduce.3");
        p0MRchildQueues.add(p0MR_3);

        FSLeafQueue p0MR_4 = mock(FSLeafQueue.class);
        apps = generateAppSchedCollectionOfSize(30);
        when(p0MR_4.getRunnableAppSchedulables()).thenReturn(apps);
        when(p0MR_4.getQueueName()).thenReturn("root.Priority0.MapReduce.4");
        p0MRchildQueues.add(p0MR_4);
        return p0MRQ;
    }

    private FSQueue generateP1Queue() {
        FSQueue p1Q = mock(FSQueue.class);
        Collection<FSQueue> p1childQueues = new ArrayList<FSQueue>();

        Collection<FSAppAttempt> apps;

        FSLeafQueue p1_0 = mock(FSLeafQueue.class);
        apps = generateAppSchedCollectionOfSize(10);
        when(p1_0.getRunnableAppSchedulables()).thenReturn(apps);
        when(p1_0.getQueueName()).thenReturn("root.Priority1.0");
        p1childQueues.add(p1_0);

        FSLeafQueue p1_1 = mock(FSLeafQueue.class);
        apps = generateAppSchedCollectionOfSize(10, DELL_APPLICATION_ID);
        when(p1_1.getRunnableAppSchedulables()).thenReturn(apps);
        when(p1_1.getQueueName()).thenReturn("root.Priority1.1");
        p1childQueues.add(p1_1);

        FSLeafQueue p1_2 = mock(FSLeafQueue.class);
        apps = generateAppSchedCollectionOfSize(10);
        when(p1_2.getRunnableAppSchedulables()).thenReturn(apps);
        when(p1_2.getQueueName()).thenReturn("root.Priority1.2");
        p1childQueues.add(p1_2);

        when(p1Q.getChildQueues()).thenReturn((List<FSQueue>) (p1childQueues));

        return p1Q;
    }

    private FSQueue generateQueueWithEmptyLeaves() {
        FSQueue emptyQ = mock(FSQueue.class);
        when(emptyQ.getQueueName()).thenReturn(PRIORITY_NO_LEAVES);
        Collection<FSQueue> emptyChildQueues = Collections.emptyList();

        when(emptyQ.getChildQueues()).thenReturn((List<FSQueue>) emptyChildQueues);

        return emptyQ;
    }

    private Collection<FSAppAttempt> generateAppSchedCollectionOfSize(int size) {
        return generateAppSchedCollectionOfSize(size, null);
    }

    private Collection<FSAppAttempt> generateAppSchedCollectionOfSize(int size, String applicationIdString) {
        Collection<FSAppAttempt> collection = new ArrayList<FSAppAttempt>();
        while (size > 0) {
            FSAppAttempt appSched = mock(FSAppAttempt.class);
            when(appSched.getName()).thenReturn(applicationIdString);
            collection.add(appSched);
            size--;
        }
        return collection;
    }

    @Test(groups = "unit")
    public void testStickyP0NonMRQueue() throws Exception {
        final String requestedQueue = "Priority0.0";

        assertEquals(queueAssigner.getAssignedQueue(requestedQueue, rmAppDell, queueManager), "root.Priority0.2");
    }

    @Test(groups = "unit")
    public void testStickyP1NonMRQueue() throws Exception {
        final String requestedQueue = "Priority1.0";

        assertEquals(queueAssigner.getAssignedQueue(requestedQueue, rmAppDell, queueManager), "root.Priority1.1");
    }

    @Test(groups = "unit")
    public void testStickyP0MRQueue() throws Exception {
        final String requestedQueue = "Priority0.MapReduce.0";

        assertEquals(queueAssigner.getAssignedQueue(requestedQueue, rmAppDell, queueManager),
                "root.Priority0.MapReduce.0");
    }

    @Test(groups = "unit")
    public void testNewlyAssignedGetLeastUtilizedMRQueue() throws Exception {
        final String requestedQueue = "Priority0.MapReduce.0";

        assertEquals(queueAssigner.getAssignedQueue(requestedQueue, rmAppNobody, queueManager),
                "root.Priority0.MapReduce.1");
    }

    @Test(groups = "unit")
    public void testNewlyAssignedGetLeastUtilizedNonMRQueue() throws Exception {
        final String requestedQueue = "Priority0.0";

        assertEquals(queueAssigner.getAssignedQueue(requestedQueue, rmAppNobody, queueManager), "root.Priority0.3");
    }

    @Test(groups = "unit")
    public void testNewlyAssignedAllEqualUtilizedNonMRQueue() throws Exception {
        final String requestedQueue = "Priority1.0";

        assertEquals(queueAssigner.getAssignedQueue(requestedQueue, rmAppNobody, queueManager), "root.Priority1.0");
    }

    @Test(groups = "unit")
    public void testNewlyAssignedRequestedParentQueueDoesNotExist() throws Exception {
        final String requestedQueue = "ThisParentQueueDoesNotExist";

        assertEquals(queueAssigner.getAssignedQueue(requestedQueue, rmAppNobody, queueManager), requestedQueue);
    }

    @Test(groups = "unit")
    public void testNewlyAssignedNoLeafQueueForParentQueue() throws Exception {
        final String requestedQueue = PRIORITY_NO_LEAVES;

        assertEquals(queueAssigner.getAssignedQueue(requestedQueue, rmAppNobody, queueManager), requestedQueue);
    }

    @Test(groups = "unit")
    public void testGetParentQueueFromFullQueueName() throws Exception {
        assertEquals(queueAssigner.getParentQueueFromFullQueueName("Priority0.2"), "Priority0");
        assertEquals(queueAssigner.getParentQueueFromFullQueueName("root.Priority0.2"), "root.Priority0");
        assertEquals(queueAssigner.getParentQueueFromFullQueueName("Priority0.MapReduce.2"), "Priority0.MapReduce");
        assertEquals(queueAssigner.getParentQueueFromFullQueueName("root.Priority0.MapReduce.2"),
                "root.Priority0.MapReduce");
        assertEquals(queueAssigner.getParentQueueFromFullQueueName("noDelimiter"), "noDelimiter");
    }

    @Test(groups = "unit")
    public void testGetQueueNameMethods() throws Exception {
        assertEquals(LedpQueueAssigner.getMRQueueNameForSubmission(), "Priority0.MapReduce.0");
        assertEquals(LedpQueueAssigner.getNonMRQueueNameForSubmission(1), "Priority1.0");
    }
}
