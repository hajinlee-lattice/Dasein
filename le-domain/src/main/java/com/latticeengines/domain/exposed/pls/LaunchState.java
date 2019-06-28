package com.latticeengines.domain.exposed.pls;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.latticeengines.domain.exposed.workflow.JobStatus;

public enum LaunchState {
    UnLaunched, //
    Queued, //
    Launching, //
    Launched, //
    Failed, //
    Canceled, //
    Deleted, //

    Syncing, //
    Synced, //
    PartialSync, //
    SyncFailed;

    private static Map<LaunchState, Collection<LaunchState>> transitionMap = new HashMap<>();

    static {
        Set<LaunchState> statesAfterUnLaunched = new HashSet<>();
        statesAfterUnLaunched.add(Launching);
        statesAfterUnLaunched.add(Queued);
        transitionMap.put(UnLaunched, statesAfterUnLaunched);

        Set<LaunchState> statesAfterQueued = new HashSet<>();
        statesAfterUnLaunched.add(Launching);
        transitionMap.put(Queued, statesAfterQueued);

        Set<LaunchState> statesAfterLaunching = new HashSet<>();
        statesAfterLaunching.add(Launched);
        statesAfterLaunching.add(Canceled);
        statesAfterLaunching.add(Failed);
        statesAfterLaunching.add(Deleted);
        transitionMap.put(Launching, statesAfterLaunching);

        Set<LaunchState> statesAfterLaunched = new HashSet<>();
        statesAfterLaunched.add(Syncing);
        statesAfterLaunched.add(Synced);
        statesAfterLaunched.add(PartialSync);
        statesAfterLaunched.add(SyncFailed);
        transitionMap.put(Launched, statesAfterLaunched);

        Set<LaunchState> statesAfterSyncing = new HashSet<>();
        statesAfterSyncing.add(Synced);
        statesAfterLaunched.add(PartialSync);
        statesAfterSyncing.add(SyncFailed);
        transitionMap.put(Syncing, statesAfterSyncing);
    }

    public static boolean canTransit(LaunchState srcState, LaunchState dstState) {
        if (transitionMap.containsKey(srcState) && transitionMap.get(srcState).contains(dstState)) {
            return true;
        }
        return false;
    }

    public static LaunchState translateFromJobStatus(JobStatus jobStatus) {
        switch (jobStatus) {
            case FAILED:
                return Failed;
            case READY:
            case PENDING:
            case RUNNING:
                return Launching;
            case SKIPPED:
            case CANCELLED:
                return Canceled;
            case COMPLETED:
                return Launched;
            default:
                return UnLaunched;
        }
    }
}
