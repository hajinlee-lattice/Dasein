package com.latticeengines.domain.exposed.pls;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public enum LaunchState {
    Launching, //
    Launched, //
    Failed, //
    Canceled, //
    Deleted;

    private static Map<LaunchState, Collection<LaunchState>> transitionMap = new HashMap<>();

    static {
        Set<LaunchState> stateAfterLaunching = new HashSet<>();
        stateAfterLaunching.add(Launched);
        stateAfterLaunching.add(Canceled);
        stateAfterLaunching.add(Failed);
        stateAfterLaunching.add(Deleted);
        transitionMap.put(Launching, stateAfterLaunching);
    }

    public static boolean canTransit(LaunchState srcState, LaunchState dstState) {
        if (transitionMap.containsKey(srcState) && transitionMap.get(srcState).contains(dstState)) {
            return true;
        }
        return false;
    }
}
