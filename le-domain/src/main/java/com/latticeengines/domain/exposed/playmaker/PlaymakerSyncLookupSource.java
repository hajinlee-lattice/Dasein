package com.latticeengines.domain.exposed.playmaker;

public enum PlaymakerSyncLookupSource {
    PLAYMAKER, // indicates to use PoetDB (old playmaker)
    LPI, // indicates to use Lpi
    DECIDED_BY_FEATURE_FLAG; // indicates to use tenant feature flag to decide
                             // where to read from
}
