package com.latticeengines.auth.exposed.service.impl;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.graphdb.DependenciesToGraphAction;
import com.latticeengines.graphdb.util.DependencyUtils;

@Component
public class GlobalAuthDependencyChecker {

    private static final Logger log = LoggerFactory.getLogger(GlobalAuthDependencyChecker.class);

    @Inject
    private DependenciesToGraphAction dependenciesToGraphAction;

    @Inject
    private GlobalAuthIdToDisplayNameTranslator idToDisplayNameTranslator;

    public Map<String, List<String>> getDependencies(String objectId, String objectType)
            throws Exception {
        log.info(String.format("Attempting to find dependencies for id = %s, type = %s", objectId, objectType));
        return DependencyUtils.getDependencies(idToDisplayNameTranslator, dependenciesToGraphAction,
                MultiTenantContext.getTenant().getId(), objectId, objectType);
    }
}
