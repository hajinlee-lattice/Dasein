package com.latticeengines.domain.exposed.metadata.mds;

interface NeedsLoad {

    void load();

    boolean isLoaded();

    default void blockingLoad() {
        if (!isLoaded()) {
            synchronized (this) {
                if (!isLoaded()) {
                    load();
                }
            }
        }
    }

}
