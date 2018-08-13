package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.PlayEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.RatingEngineEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.SegmentEntityMgr;
import com.latticeengines.domain.exposed.cdl.CDLObjectTypes;
import com.latticeengines.domain.exposed.graph.GraphConstants;
import com.latticeengines.domain.exposed.graph.NameSpaceUtil;
import com.latticeengines.domain.exposed.graph.VertexType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;

@Component
public class IdToDisplayNameTranslator {

    public static final String ID = "id";
    public static final String DISPLAY_NAME = "displayName";
    public static final String TYPE = "type";

    @Inject
    private SegmentEntityMgr segmentEntityMgr;

    @Inject
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    @Inject
    private PlayEntityMgr playEntityMgr;

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
                        result.get(translatedType).add(objInfo);
                    });
            result.keySet().stream() //
                    .forEach(type -> {
                        result.get(type).stream() //
                                .forEach(objInfo -> {
                                    String objId = objInfo.get(ID);
                                    String displayName = idToDisplayName(type, objId);
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
                                    String displayName = idToDisplayName(translatedType, objId);
                                    objInfo.put(DISPLAY_NAME, displayName);
                                    objInfo.put(TYPE, translatedType);
                                    pathInfo.add(objInfo);
                                });
                    });
        }
        return result;
    }

    public String idToDisplayName(String type, String objId) {
        String displayName = objId;
        if (type.equals(CDLObjectTypes.Play.name())) {
            Play play = playEntityMgr.getPlayByName(objId, false);
            if (play != null) {
                displayName = play.getDisplayName();
            }
        } else if (type.equals(CDLObjectTypes.Model.name())) {
            RatingEngine ratingEngine = ratingEngineEntityMgr.findById(objId);
            if (ratingEngine != null) {
                displayName = ratingEngine.getDisplayName();
            }
        } else if (type.equals(CDLObjectTypes.Segment)) {
            MetadataSegment segment = segmentEntityMgr.findByName(objId);
            if (segment != null) {
                displayName = segment.getDisplayName();
            }
        }
        return displayName;
    }

    public String translateType(String vertexType) {
        String translatedType = vertexType;
        if (vertexType.equals(VertexType.PLAY)) {
            translatedType = CDLObjectTypes.Play.name();
        } else if (vertexType.equals(VertexType.RATING_ENGINE)) {
            translatedType = CDLObjectTypes.Model.name();
        } else if (vertexType.equals(VertexType.SEGMENT)) {
            translatedType = CDLObjectTypes.Segment.name();
        }
        return translatedType;
    }

    public String toVertexType(String objectType) {
        String translatedType = null;
        CDLObjectTypes type = CDLObjectTypes.valueOf(objectType);
        if (type != null) {
            if (type == CDLObjectTypes.Play) {
                translatedType = VertexType.PLAY;
            } else if (type == CDLObjectTypes.Model) {
                translatedType = VertexType.RATING_ENGINE;
            } else if (type == CDLObjectTypes.Segment) {
                translatedType = VertexType.SEGMENT;
            }
        }
        return translatedType;
    }
}
