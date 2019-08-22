package com.latticeengines.apps.lp.qbean;

import com.amazonaws.services.elasticmapreduce.model.InstanceFleet;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroup;
import com.latticeengines.aws.emr.EMRService;

class EMRTracker {

    private EMRScaleGroup coreGrp;
    private EMRScaleGroup taskGrp;

    private final String clusterId;
    private final EMRService emrService;

    EMRTracker( String clusterId, EMRService emrService) {
        this.clusterId = clusterId;
        this.emrService = emrService;
    }

    void clear() {
        this.coreGrp = null;
        this.taskGrp = null;
    }

    // ====================
    // CORE Nodes
    // ====================

    void trackCoreGrp(InstanceGroup grp) {
        coreGrp = new EMRScaleGroup(grp);
    }

    void trackCoreFleet(InstanceFleet fleet) {
        coreGrp = new EMRScaleGroup(fleet);
    }

    long getCoreMb() {
        return coreGrp.getNodeMb();
    }

    int getCoreVCores() {
        return coreGrp.getNodeVCores();
    }

    int getRunningCore() {
        return coreGrp.getRunning();
    }

    // ====================
    // TASK Nodes
    // ====================

    void trackTaskGrp(InstanceGroup grp) {
        taskGrp = new EMRScaleGroup(grp);
    }

    void trackTaskFleet(InstanceFleet fleet) {
        taskGrp = new EMRScaleGroup(fleet);
    }

    long getTaskMb() {
        return taskGrp.getNodeMb();
    }

    int getTaskVCores() {
        return taskGrp.getNodeVCores();
    }

    int getRunningTask() {
        return taskGrp.getRunning();
    }

    int getRequestedTask() {
        return taskGrp.getRequested();
    }

    void scale(int target) {
        if (taskGrp.getFleet() != null) {
            emrService.scaleTaskFleet(clusterId, taskGrp.getFleet(), 0, target);
        } else {
            emrService.scaleTaskGroup(clusterId, taskGrp.getGrp(), target);
        }
    }

}
