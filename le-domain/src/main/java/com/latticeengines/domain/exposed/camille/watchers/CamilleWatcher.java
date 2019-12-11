package com.latticeengines.domain.exposed.camille.watchers;

public enum CamilleWatcher {
    // Data Cloud
    // To trigger refreshing base cache of all the DataCloud metadata; Change
    // should be triggered before AMRelease
    AMReleaseBaseCache,
    // To trigger refreshing upper-layer cache which is built upon base cache;
    // Change should be triggered after AMReleaseBaseCache and wait for a short
    // silent period after base cache finishes loading
    AMRelease,
    // To trigger refreshing DnB token in redis by requesting
    // from DnB, then refreshing token cached locally in tomcat matchapi
    // services or yarn containers
    DnBToken

}
