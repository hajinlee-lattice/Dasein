package com.latticeengines.domain.exposed.camille.watchers;

public enum CamilleWatcher {

    // Data Cloud
    AMRelease,      // turn on a new version of AM (metadata, dynamo, redshift, ...)
    AMApiUpdate,    // refresh all cached proxies

    // CDL
    CDLConsolidate, // a new data collection consolidate just finished
    CDLProfile,     // a new data collection profile just finished
}
