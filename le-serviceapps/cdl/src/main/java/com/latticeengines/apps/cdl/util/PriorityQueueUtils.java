package com.latticeengines.apps.cdl.util;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.ActivityObject;
import com.latticeengines.domain.exposed.cdl.PriorityObject;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantType;

public class PriorityQueueUtils {

    private static final Logger log = LoggerFactory.getLogger(PriorityQueueUtils.class);

    private static PriorityBlockingQueue<PriorityObject> highPriorityQueue = new PriorityBlockingQueue<>();
    private static PriorityBlockingQueue<PriorityObject> lowPriorityQueue = new PriorityBlockingQueue<>();
    private static Map<String, PriorityObject> highPriorityMap = new HashMap<>();
    private static Map<String, PriorityObject> lowPriorityMap = new HashMap<>();


    public static void createOrUpdateQueue(List<ActivityObject> activityObjects) {
        if (CollectionUtils.isEmpty(activityObjects)) {
            return;
        }
        for (ActivityObject activityObject : activityObjects) {
            push(activityObject);
        }
    }

    public static void push(ActivityObject activityObject) {
        Tenant tenant = activityObject.getTenant();
        if (tenant != null) {
            String uniqueKey = tenant.getName();
            try {
                if (!isValid(activityObject)) {
                    if (highPriorityMap.containsKey(uniqueKey)) {
                        PriorityObject originObject = highPriorityMap.get(uniqueKey);
                        highPriorityQueue.remove(originObject);
                        highPriorityMap.remove(uniqueKey, originObject);
                    }
                    if (lowPriorityMap.containsKey(uniqueKey)) {
                        PriorityObject originObject = lowPriorityMap.get(uniqueKey);
                        lowPriorityQueue.remove(originObject);
                        lowPriorityMap.remove(uniqueKey, originObject);
                    }
                } else {
                    PriorityObject currentObject = convertObject(activityObject);
                    if (currentObject.getScheduleNow() == 1 || tenant.getTenantType() == TenantType.CUSTOMER) {
                        if (highPriorityMap.containsKey(uniqueKey)) {
                            PriorityObject originObject = highPriorityMap.get(uniqueKey);
                            if (!currentObject.objectEquals(originObject)) {
                                highPriorityQueue.remove(originObject);
                                highPriorityQueue.offer(currentObject);
                                highPriorityMap.put(uniqueKey, currentObject);
                            }
                        } else {
                            highPriorityQueue.offer(currentObject);
                            highPriorityMap.put(uniqueKey, currentObject);
                        }
                    }
                    if (currentObject.getScheduleNow() == 0) {
                        if (lowPriorityMap.containsKey(uniqueKey)) {
                            PriorityObject originObject = lowPriorityMap.get(uniqueKey);
                            if (!currentObject.objectEquals(originObject)) {
                                lowPriorityQueue.remove(originObject);
                                lowPriorityQueue.offer(currentObject);
                                lowPriorityMap.put(uniqueKey, currentObject);
                            }
                        } else {
                            lowPriorityQueue.offer(currentObject);
                            lowPriorityMap.put(uniqueKey, currentObject);
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                log.error(e.getMessage());
                log.info("this activityObject is : " + JsonUtils.serialize(activityObject));
            }
        } else {
            log.warn("this activityObject cannot find tenant. activityObject is : " + JsonUtils.serialize(activityObject));
        }
    }

    public static String pickFirstFromHighPriority() {
        if (highPriorityQueue.size() < 1) {
            return null;
        } else {
            PriorityObject firstObject = highPriorityQueue.peek();
            return firstObject.getTenantName();
        }
    }

    public static String pollFirstFromHighPriority() {
        if (highPriorityQueue.size() < 1) {
            return null;
        } else {
            PriorityObject firstObject = highPriorityQueue.poll();
            highPriorityMap.remove(firstObject.getTenantName(), firstObject);
            return firstObject.getTenantName();
        }
    }

    public static int getHighPriorityQueueSize() {
        return highPriorityQueue.size();
    }

    public static int getPositionFromHighPriorityQueue(String tenantName) {
        if (highPriorityMap.containsKey(tenantName)) {
            PriorityObject currentObject = highPriorityMap.get(tenantName);
            List<PriorityObject> priorityQueues = new LinkedList<>();
            int index = 0;
            while (highPriorityQueue.peek() != null) {
                PriorityObject priorityObject = highPriorityQueue.poll();
                priorityQueues.add(priorityObject);
                if (priorityObject.objectEquals(currentObject)) {
                    highPriorityQueue.addAll(priorityQueues);
                    return index;
                }
                index++;
            }
        }
        return -1;
    }

    public static List<String> getAllMemberWithSortFromHighPriorityQueue() {
        List<String> tenantNameList = new ArrayList<>();
        List<PriorityObject> priorityQueues = new LinkedList<>();
        while (highPriorityQueue.peek() != null) {
            PriorityObject priorityObject = highPriorityQueue.poll();
            priorityQueues.add(priorityObject);
            tenantNameList.add(priorityObject.getTenantName());
        }
        highPriorityQueue.addAll(priorityQueues);
        return tenantNameList;
    }

    public static String pickFirstFromLowPriority() {
        if (lowPriorityQueue.size() < 1) {
            return null;
        } else {
            PriorityObject firstObject = lowPriorityQueue.peek();
            return firstObject.getTenantName();
        }
    }

    public static String pollFirstFromLowPriority() {
        if (lowPriorityQueue.size() < 1) {
            return null;
        } else {
            PriorityObject firstObject = lowPriorityQueue.poll();
            lowPriorityMap.remove(firstObject.getTenantName(), firstObject);
            return firstObject.getTenantName();
        }
    }

    public static int getLowPriorityQueueSize() {
        return lowPriorityQueue.size();
    }

    public static int getPositionFromLowPriorityQueue(String tenantName) {
        if (lowPriorityMap.containsKey(tenantName)) {
            PriorityObject currentObject = lowPriorityMap.get(tenantName);
            List<PriorityObject> priorityQueues = new LinkedList<>();
            int index = 0;
            while (lowPriorityQueue.peek() != null) {
                PriorityObject priorityObject = lowPriorityQueue.poll();
                priorityQueues.add(priorityObject);
                if (priorityObject.objectEquals(currentObject)) {
                    lowPriorityQueue.addAll(priorityQueues);
                    return index;
                }
                index++;
            }
        }
        return -1;
    }

    public static List<String> getAllMemberWithSortFromLowPriorityQueue() {
        List<String> tenantNameList = new ArrayList<>();
        List<PriorityObject> priorityQueues = new LinkedList<>();
        while (lowPriorityQueue.peek() != null) {
            PriorityObject priorityObject = lowPriorityQueue.poll();
            priorityQueues.add(priorityObject);
            tenantNameList.add(priorityObject.getTenantName());
        }
        lowPriorityQueue.addAll(priorityQueues);
        return tenantNameList;
    }

    private static PriorityObject convertObject(ActivityObject activityObject) {
        PriorityObject newObject = new PriorityObject();
        long currentTime = new Date().getTime();
        if (activityObject.getDataCloudRefresh() == null) {
            newObject.setDataCloudRefresh(0);
        } else {
            newObject.setDataCloudRefresh(activityObject.getDataCloudRefresh() ? 1 : 0);
        }
        newObject.setInvokeTime(activityObject.getInvokeTime() != null ? activityObject.getInvokeTime().getTime() : currentTime);
        if (activityObject.getScheduleNow() == null) {
            newObject.setScheduleNow(0);
            newObject.setScheduleTime(currentTime);
        } else {
            newObject.setScheduleNow(activityObject.getScheduleNow() ? 1 : 0);
            newObject.setScheduleTime(activityObject.getScheduleTime() != null ?
                    activityObject.getScheduleTime().getTime() : currentTime);
        }
        newObject.setTenantName(activityObject.getTenant().getName());
        long actionTime = currentTime;
        if (activityObject.getActions() == null) {
            newObject.setFirstActionTime(actionTime);
        } else {
            for (Action action : activityObject.getActions()) {
                if (actionTime > action.getCreated().getTime()) {
                    actionTime = action.getCreated().getTime();
                }
            }
            newObject.setFirstActionTime(actionTime);
        }
        return newObject;
    }

    private static boolean isValid(ActivityObject activityObject) {
        if (activityObject.getScheduleNow() != null && activityObject.getScheduleNow()) {
            return true;
        }
        if ((activityObject.getDataCloudRefresh() != null && activityObject.getDataCloudRefresh()) || activityObject.getInvokeTime() != null) {
            List<Action> actionList = activityObject.getActions();
            if (actionList != null && actionList.size() > 0) {
                long lastActionTime = 0L;
                for (Action action : activityObject.getActions()) {
                    if (action.getCreated().getTime() > lastActionTime) {
                        lastActionTime = action.getCreated().getTime();
                    }
                }
                long minute = new Date().getTime();
                minute = (minute - lastActionTime) /60000;
                return lastActionTime != 0L && minute >= 10;
            }
        }
        return false;
    }

    public static void removeActivityFromQueue(String tenantName) {
        if (highPriorityMap.containsKey(tenantName)) {
            PriorityObject priorityObject = highPriorityMap.get(tenantName);
            highPriorityQueue.remove(priorityObject);
            highPriorityMap.remove(tenantName, priorityObject);
        }
        if (lowPriorityMap.containsKey(tenantName)) {
            PriorityObject priorityObject = lowPriorityMap.get(tenantName);
            lowPriorityQueue.remove(priorityObject);
            lowPriorityMap.remove(tenantName, priorityObject);
        }
    }

}
