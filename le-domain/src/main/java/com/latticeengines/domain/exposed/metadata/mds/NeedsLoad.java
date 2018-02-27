package com.latticeengines.domain.exposed.metadata.mds;

import reactor.core.publisher.Mono;

interface NeedsLoad {

    Mono<Boolean> load();

    boolean isLoaded();

    default void blockingLoad() {
        if (!isLoaded()) {
            synchronized (this) {
                if (!isLoaded()) {
                    load().block();
                }
            }
        }
    }

}
