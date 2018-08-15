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
import com.latticeengines.apps.cdl.mds.RatingDisplayMetadataStore;
import com.latticeengines.domain.exposed.cdl.CDLObjectTypes;
import com.latticeengines.domain.exposed.graph.GraphConstants;
import com.latticeengines.domain.exposed.graph.NameSpaceUtil;
import com.latticeengines.domain.exposed.graph.VertexType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngine.ScoreType;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component
public class IdToDisplayNameTranslator {

    public static final String RATING_ATTRIBUTE = "Rating Attribute";
    public static final String ID = "id";
    public static final String DISPLAY_NAME = "displayName";
    public static final String TYPE = "type";

    @Inject
    private SegmentEntityMgr segmentEntityMgr;

    @Inject
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    @Inject
    private PlayEntityMgr playEntityMgr;

    @Inject
    private RatingDisplayMetadataStore ratingDisplayMetadataStore;

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

    public String idToDisplayName(String type, String objId) {
        String displayName = objId;
        if (type.equals(VertexType.PLAY)) {
            Play play = playEntityMgr.getPlayByName(objId, false);
            if (play != null) {
                displayName = play.getDisplayName();
            }
        } else if (type.equals(VertexType.RATING_ENGINE)) {
            RatingEngine ratingEngine = ratingEngineEntityMgr.findById(objId);
            if (ratingEngine != null) {
                displayName = ratingEngine.getDisplayName();
            }
        } else if (type.equals(VertexType.SEGMENT)) {
            MetadataSegment segment = segmentEntityMgr.findByName(objId);
            if (segment != null) {
                displayName = segment.getDisplayName();
            }
        } else if (type.equals(VertexType.RATING_ATTRIBUTE) //
                || type.equals(VertexType.RATING_SCORE_ATTRIBUTE) //
                || type.equals(VertexType.RATING_PROB_ATTRIBUTE) //
                || type.equals(VertexType.RATING_EV_ATTRIBUTE)) {
            String displayNameSuffix = "";
            String modelId = objId.substring((BusinessEntity.Rating + ".").length());
            ScoreType scoreType = ScoreType.Rating;
            if (type.equals(VertexType.RATING_SCORE_ATTRIBUTE)) {
                scoreType = ScoreType.Score;
            } else if (type.equals(VertexType.RATING_PROB_ATTRIBUTE)) {
                scoreType = ScoreType.Probability;
            } else if (type.equals(VertexType.RATING_EV_ATTRIBUTE)) {
                scoreType = ScoreType.ExpectedRevenue;
            }

            if (scoreType != ScoreType.Rating) {
                modelId = modelId.substring(0,
                        modelId.lastIndexOf( //
                                "_" + RatingEngine.SCORE_ATTR_SUFFIX //
                                        .get(scoreType)));
            }

            RatingEngine model = ratingEngineEntityMgr.findById(modelId);
            if (model != null) {
                displayName = model.getDisplayName();
            }

            if (scoreType != ScoreType.Rating) {
                displayNameSuffix = " " + ratingDisplayMetadataStore.getSecondaryDisplayName("_" + scoreType.name());
            }
            displayName += displayNameSuffix;
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
        } else if (vertexType.equals(VertexType.RATING_ATTRIBUTE)) {
            translatedType = RATING_ATTRIBUTE;
        } else if (vertexType.equals(VertexType.RATING_SCORE_ATTRIBUTE)) {
            translatedType = RATING_ATTRIBUTE;
        } else if (vertexType.equals(VertexType.RATING_EV_ATTRIBUTE)) {
            translatedType = RATING_ATTRIBUTE;
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
