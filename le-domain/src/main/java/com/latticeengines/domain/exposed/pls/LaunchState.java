package com.latticeengines.domain.exposed.pls;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.latticeengines.domain.exposed.workflow.JobStatus;

public enum LaunchState {
    UnLaunched(true, false), //
    Queued(true, false), //
    PreProcessing(false, false), //
    Skipped(false, true), //
    Launching(false, false), //
    Launched(false, true), //
    Failed(true, true), //
    Canceled(true, true), //

    Syncing(false, false), //
    Synced(false, true), //
    PartialSync(false, true), //
    SyncFailed(false, true);

    private static Map<LaunchState, Collection<LaunchState>> transitionMap = new HashMap<>();

    static {

        Set<LaunchState> statesAfterPreProcessing = new HashSet<>();
        statesAfterPreProcessing.add(Skipped);
        statesAfterPreProcessing.add(Canceled);
        statesAfterPreProcessing.add(Failed);
        statesAfterPreProcessing.add(Queued);
        transitionMap.put(Launching, statesAfterPreProcessing);

        Set<LaunchState> statesAfterLaunching = new HashSet<>();
        statesAfterLaunching.add(Launched);
        statesAfterLaunching.add(Canceled);
        statesAfterLaunching.add(Failed);
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
        return transitionMap.containsKey(srcState) && transitionMap.get(srcState).contains(dstState);
    }

    private boolean initial;
    private boolean terminal;

    LaunchState(boolean initial, boolean terminal) {
        this.initial = initial;
        this.terminal = terminal;
    }

    public static LaunchState translateFromJobStatus(JobStatus jobStatus) {
        switch (jobStatus) {
        case FAILED:
            return Failed;
        case READY:
        case PENDING:
        case RUNNING:
        case ENQUEUED:
        case PENDING_RETRY:
        case RETRIED:
            return Launching;
        case SKIPPED:
        case CANCELLED:
            return Canceled;
        case COMPLETED:
            return Launched;
        default:
            return Canceled;
        }
    }

    public Boolean isInitial() {
        return this.initial;
    }

    public Boolean isTerminal() {
        return this.terminal;
    }
}
