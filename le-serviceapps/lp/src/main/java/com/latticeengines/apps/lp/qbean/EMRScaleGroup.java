package com.latticeengines.apps.lp.qbean;

import com.amazonaws.services.elasticmapreduce.model.InstanceFleet;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroup;
import com.latticeengines.domain.exposed.aws.EC2InstanceType;

public class EMRScaleGroup {

    private final long nodeMb;
    private final int nodeVCores;

    private final InstanceGroup grp;
    private final InstanceFleet fleet;

    EMRScaleGroup(InstanceGroup grp) {
        this.grp = grp;
        this.fleet = null;

        String instanceType = grp.getInstanceType();
        long[] cap = getEc2Capacity(instanceType);
        this.nodeMb = cap[0];
        this.nodeVCores = (int) cap[1];
    }

    EMRScaleGroup(InstanceFleet fleet) {
        this.grp = null;
        this.fleet = fleet;

        String instanceType = fleet.getInstanceTypeSpecifications().get(0).getInstanceType();
        long[] cap = getEc2Capacity(instanceType);
        this.nodeMb = cap[0];
        this.nodeVCores = (int) cap[1];
    }

    private static long[] getEc2Capacity(String instanceType) {
        EC2InstanceType ec2InstanceType = EC2InstanceType.fromName(instanceType);
        if (ec2InstanceType == null) {
            throw new UnsupportedOperationException("Instance type " + instanceType + " is not defined.");
        }
        long nodeMb = (long) (ec2InstanceType.getMemGb() - 8) * 1024;
        long nodeVCores = (long) ec2InstanceType.getvCores();
        return new long[] {nodeMb, nodeVCores};
    }

    public InstanceGroup getGrp() {
        return grp;
    }

    public InstanceFleet getFleet() {
        return fleet;
    }

    long getNodeMb() {
        return nodeMb;
    }

    int getNodeVCores() {
        return nodeVCores;
    }

    int getRunning() {
        if (fleet != null) {
            return fleet.getProvisionedOnDemandCapacity() + fleet.getProvisionedSpotCapacity();
        } else {
            return grp.getRunningInstanceCount();
        }
    }

    int getRequested() {
        if (fleet != null) {
            return fleet.getTargetOnDemandCapacity() + fleet.getTargetSpotCapacity();
        } else {
            return grp.getRequestedInstanceCount();
        }
    }

}
