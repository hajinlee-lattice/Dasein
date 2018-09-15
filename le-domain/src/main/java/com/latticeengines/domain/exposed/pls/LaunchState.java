package com.latticeengines.domain.exposed.pls;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public enum LaunchState {
    UnLaunched, //
    Launching, //
    Launched, //
    Failed, //
    Canceled, //
    Deleted;

    private static Map<LaunchState, Collection<LaunchState>> transitionMap = new HashMap<>();

    static {
        Set<LaunchState> statesAfterUnLaunched = new HashSet<>();
        statesAfterUnLaunched.add(Launching);
        transitionMap.put(UnLaunched, statesAfterUnLaunched);

        Set<LaunchState> statesAfterLaunching = new HashSet<>();
        statesAfterLaunching.add(Launched);
        statesAfterLaunching.add(Canceled);
        statesAfterLaunching.add(Failed);
        statesAfterLaunching.add(Deleted);
        transitionMap.put(Launching, statesAfterLaunching);
    }

    public static boolean canTransit(LaunchState srcState, LaunchState dstState) {
        if (transitionMap.containsKey(srcState) && transitionMap.get(srcState).contains(dstState)) {
            return true;
        }
        return false;
    }
}
