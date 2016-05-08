package com.latticeengines.propdata.match.service.impl;

import javax.annotation.PostConstruct;

import com.latticeengines.propdata.match.service.EmbeddedDbService;

public class EmbeddedDbLoader {

    private EmbeddedDbService embeddedDbService;

    private Boolean preloadEnabled;

    @PostConstruct
    private void postConstruct() {
        if (preloadEnabled) {
            embeddedDbService.loadAsync();
        }
    }

    public void setPreloadEnabled(Boolean preloadEnabled) {
        this.preloadEnabled = preloadEnabled;
    }

    public void setEmbeddedDbService(EmbeddedDbService embeddedDbService) {
        this.embeddedDbService = embeddedDbService;
    }
}
