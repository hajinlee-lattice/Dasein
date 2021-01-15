package com.latticeengines.apps.lp.qbean;

import com.amazonaws.services.elasticmapreduce.model.InstanceFleet;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroup;

public class EMRScaleGroup {

    private final InstanceGroup grp;
    private final InstanceFleet fleet;

    EMRScaleGroup(InstanceGroup grp) {
        this.grp = grp;
        this.fleet = null;
    }

    EMRScaleGroup(InstanceFleet fleet) {
        this.grp = null;
        this.fleet = fleet;
    }

    public InstanceGroup getGrp() {
        return grp;
    }

    public InstanceFleet getFleet() {
        return fleet;
    }

    int getRunning() {
        if (fleet != null) {
            return fleet.getProvisionedOnDemandCapacity() + fleet.getProvisionedSpotCapacity();
        } else if (grp != null) {
            return grp.getRunningInstanceCount();
        } else {
            throw new IllegalStateException("grp and fleet cannot both be null.");
        }
    }

    int getRequested() {
        if (fleet != null) {
            return fleet.getTargetOnDemandCapacity() + fleet.getTargetSpotCapacity();
        } else if (grp != null) {
            return grp.getRequestedInstanceCount();
        } else {
            throw new IllegalStateException("grp and fleet cannot both be null.");
        }
    }

}
