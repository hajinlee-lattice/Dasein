package com.latticeengines.domain.exposed.camille.watchers;

public enum CamilleWatchers {

    // Data Cloud
    AMRelease,      // turn on a new version of AM (metadata, dynamo, redshift, ...)
    AMMedataUpdate, // keep AM data, some change in metadata
    AMApiUpdate     // refresh all cached proxies

}
