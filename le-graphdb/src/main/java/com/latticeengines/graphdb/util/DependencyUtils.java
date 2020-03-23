package com.latticeengines.graphdb.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.graph.IdToDisplayNameTranslator;
import com.latticeengines.graphdb.DependenciesToGraphAction;

public final class DependencyUtils {

    protected DependencyUtils() {
        throw new UnsupportedOperationException();
    }

    public static void checkDeleteSafety(List<Map<String, String>> dependencies, IdToDisplayNameTranslator nameTranslator, String vertexId, String vertexType) {
        if (CollectionUtils.isNotEmpty(dependencies)) {
            Map<String, List<Map<String, String>>> translatedDependenciesWithId = nameTranslator
                    .translate(dependencies);
            Map<String, List<String>> translatedDependencies = new HashMap<>();
            translatedDependenciesWithId.keySet().stream() //
                    .filter(type -> CollectionUtils.isNotEmpty(translatedDependenciesWithId.get(type))) //
                    .forEach(type -> {
                        translatedDependencies.put(type, new ArrayList<>());
                        translatedDependenciesWithId.get(type).stream().forEach(dep -> {
                            translatedDependencies.get(type) //
                                    .add(dep.get(IdToDisplayNameTranslator.DISPLAY_NAME));
                        });
                    });
            throw new LedpException(LedpCode.LEDP_40042, new String[]{JsonUtils.serialize(translatedDependencies)});
        }
    }

    public static Map<String, List<String>> getDependencies(IdToDisplayNameTranslator idToDisplayNameTranslator,
                                                            DependenciesToGraphAction dependenciesToGraphAction,
                                                            String tenantId, String objectId, String objectType) throws Exception {
        HashMap<String, List<String>> dependencyMap = new HashMap<>();
        if (idToDisplayNameTranslator.toVertexType(objectType) != null) {
            List<Map<String, String>> dependencies = //
                    dependenciesToGraphAction.checkDirectDependencies(//
                            tenantId, objectId,
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
