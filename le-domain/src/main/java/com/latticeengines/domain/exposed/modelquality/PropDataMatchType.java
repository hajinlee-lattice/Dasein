package com.latticeengines.domain.exposed.modelquality;

/**
 * This match type is for the AnalyticTest. It is a higher level encapsulation of what kind of prop data matching
 * options are available for a particular test that will be applied to all data sets and analytic pipelines
 * associated with this AnalyticTest instance.
 * 
 * @author rgonzalez
 *
 */
public enum PropDataMatchType {

    DNB, //
    RTS, //
    NOMATCH
}
