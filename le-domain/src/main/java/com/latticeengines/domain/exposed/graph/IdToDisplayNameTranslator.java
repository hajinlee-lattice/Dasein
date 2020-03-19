package com.latticeengines.domain.exposed.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;

public abstract class IdToDisplayNameTranslator {
    public static final String ID = "id";
    public static final String DISPLAY_NAME = "displayName";
    public static final String TYPE = "type";

    public Map<String, List<Map<String, String>>> translate(List<Map<String, String>> inputList) {
        Map<String, List<Map<String, String>>> result = new HashMap<>();
        if (CollectionUtils.isNotEmpty(inputList)) {
            inputList.stream() //
                    .forEach(in -> {
                        String objId = in.get(GraphConstants.OBJECT_ID_KEY);
                        String objType = in.get(NameSpaceUtil.TYPE_KEY);
                        String translatedType = translateType(objType);
                        if (!result.containsKey(translatedType)) {
                            result.put(translatedType, new ArrayList<>());
                        }
                        Map<String, String> objInfo = new HashMap<>();
                        objInfo.put(ID, objId);
                        objInfo.put(TYPE, objType);
                        result.get(translatedType).add(objInfo);
                    });
            result.keySet().stream() //
                    .forEach(type -> {
                        result.get(type).stream() //
                                .forEach(objInfo -> {
                                    String objId = objInfo.get(ID);
                                    String displayName = idToDisplayName(objInfo.get(TYPE), objId);
                                    objInfo.put(DISPLAY_NAME, displayName);
                                });
                    });

        }
        return result;
    }

    public List<List<Map<String, String>>> translatePaths(List<List<Map<String, String>>> inputList) {
        List<List<Map<String, String>>> result = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(inputList)) {
            inputList.stream() //
                    .forEach(path -> {
                        List<Map<String, String>> pathInfo = new ArrayList<>();
                        result.add(pathInfo);
                        path.stream() //
                                .forEach(in -> {
                                    Map<String, String> objInfo = new HashMap<>();
                                    String objId = in.get(GraphConstants.OBJECT_ID_KEY);
                                    String objType = in.get(NameSpaceUtil.TYPE_KEY);
                                    String translatedType = translateType(objType);
                                    String displayName = idToDisplayName(objType, objId);
                                    objInfo.put(DISPLAY_NAME, displayName);
                                    objInfo.put(TYPE, translatedType);
                                    pathInfo.add(objInfo);
                                });
                    });
        }
        return result;
    }

    public abstract String idToDisplayName(String type, String objId);

    public abstract String translateType(String vertexType);

    public abstract String toVertexType(String objectType);
}
