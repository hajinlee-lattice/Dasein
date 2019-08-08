package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.graphdb.DependenciesToGraphAction;

@Component
public class DependencyChecker {

    private static Logger log = LoggerFactory.getLogger(DependencyChecker.class);

    @Inject
    private DependenciesToGraphAction dependenciesToGraphAction;

    @Inject
    private IdToDisplayNameTranslator idToDisplayNameTranslator;

    public Map<String, List<String>> getDependencies(String customerSpace, String objectId, String objectType)
            throws Exception {
        log.info(String.format("Attempting to find dependencies for id = %s, type = %s", objectId, objectType));

        HashMap<String, List<String>> dependencyMap = new HashMap<>();
        if (idToDisplayNameTranslator.toVertexType(objectType) != null) {
            List<Map<String, String>> dependencies = //
                    dependenciesToGraphAction.checkDirectDependencies(//
                            MultiTenantContext.getTenant().getId(), objectId,
                            idToDisplayNameTranslator.toVertexType(objectType));
            Map<String, List<Map<String, String>>> translatedDependencies = //
                    idToDisplayNameTranslator.translate(dependencies);

            if (MapUtils.isNotEmpty(translatedDependencies)) {
                translatedDependencies.keySet().stream() //
                        .forEach(k -> {
                            if (CollectionUtils.isNotEmpty(translatedDependencies.get(k))) {
                                dependencyMap.put(k, new ArrayList<>());
                                translatedDependencies.get(k).stream() //
                                        .forEach(v -> dependencyMap.get(k)
                                                .add(v.get(IdToDisplayNameTranslator.DISPLAY_NAME)));
                            }
                        });
            }
        }
        return dependencyMap;
    }
}
