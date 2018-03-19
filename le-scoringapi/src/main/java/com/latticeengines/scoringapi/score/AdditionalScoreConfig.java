package com.latticeengines.scoringapi.score;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public class AdditionalScoreConfig {
    private CustomerSpace space;
    private String requestId;
    private boolean isDebug;
    private boolean enrichInternalAttributes;
    private boolean performFetchOnlyForMatching;
    private boolean skipAnyMatching;
    private boolean enforceFuzzyMatch;
    private boolean skipDnBCache;
    private boolean isCalledViaApiConsole;
    private boolean isHomogeneous;
    private boolean shouldReturnAllEnrichment;
    private boolean forceSkipMatching;

    public static AdditionalScoreConfig instance() {
        return new AdditionalScoreConfig();
    }

    public CustomerSpace getSpace() {
        return space;
    }

    public AdditionalScoreConfig setSpace(CustomerSpace space) {
        this.space = space;
        return this;
    }

    public String getRequestId() {
        return requestId;
    }

    public AdditionalScoreConfig setRequestId(String requestId) {
        this.requestId = requestId;
        return this;
    }

    public boolean isDebug() {
        return isDebug;
    }

    public AdditionalScoreConfig setDebug(boolean isDebug) {
        this.isDebug = isDebug;
        return this;
    }

    public boolean isEnrichInternalAttributes() {
        return enrichInternalAttributes;
    }

    public AdditionalScoreConfig setEnrichInternalAttributes(boolean enrichInternalAttributes) {
        this.enrichInternalAttributes = enrichInternalAttributes;
        return this;
    }

    public boolean isPerformFetchOnlyForMatching() {
        return performFetchOnlyForMatching;
    }

    public AdditionalScoreConfig setPerformFetchOnlyForMatching(boolean performFetchOnlyForMatching) {
        this.performFetchOnlyForMatching = performFetchOnlyForMatching;
        return this;
    }

    public boolean isSkipAnyMatching() {
        return skipAnyMatching;
    }

    public AdditionalScoreConfig setSkipAnyMatching(boolean skipAnyMatching) {
        this.skipAnyMatching = skipAnyMatching;
        return this;
    }

    public boolean isCalledViaApiConsole() {
        return isCalledViaApiConsole;
    }

    public AdditionalScoreConfig setCalledViaApiConsole(boolean isCalledViaApiConsole) {
        this.isCalledViaApiConsole = isCalledViaApiConsole;
        return this;
    }

    public boolean isEnforceFuzzyMatch() {
        return enforceFuzzyMatch;
    }

    public AdditionalScoreConfig setEnforceFuzzyMatch(boolean enforceFuzzyMatch) {
        this.enforceFuzzyMatch = enforceFuzzyMatch;
        return this;
    }

    public boolean isSkipDnBCache() {
        return skipDnBCache;
    }

    public AdditionalScoreConfig setSkipDnBCache(boolean skipDnBCache) {
        this.skipDnBCache = skipDnBCache;
        return this;
    }

    public boolean isHomogeneous() {
        return isHomogeneous;
    }

    public AdditionalScoreConfig setHomogeneous(boolean isHomogeneous) {
        this.isHomogeneous = isHomogeneous;
        return this;
    }

    public boolean isShouldReturnAllEnrichment() {
        return shouldReturnAllEnrichment;
    }

    public AdditionalScoreConfig setShouldReturnAllEnrichment(boolean shouldReturnAllEnrichment) {
        this.shouldReturnAllEnrichment = shouldReturnAllEnrichment;
        return this;
    }

    public boolean isForceSkipMatching() {
        return forceSkipMatching;
    }

    public AdditionalScoreConfig setForceSkipMatching(boolean forceSkipMatching) {
        this.forceSkipMatching = forceSkipMatching;
        return this;
    }

}
