package com.latticeengines.domain.exposed.camille.watchers;

public enum CamilleWatcher {

    // Data Cloud
    AMRelease,      // turn on a new version of AM (metadata, dynamo, redshift, ...)
    AMApiUpdate,    // refresh all cached proxies

    // CDL
    CustomerStats,   // update data lake stats for a customer
    CustomerMetadata // update table metadata in data collection

}
