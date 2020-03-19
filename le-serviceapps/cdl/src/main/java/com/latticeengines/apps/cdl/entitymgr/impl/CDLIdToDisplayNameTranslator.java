package com.latticeengines.apps.cdl.entitymgr.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.PlayEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.RatingEngineEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.SegmentEntityMgr;
import com.latticeengines.apps.cdl.mds.RatingDisplayMetadataStore;
import com.latticeengines.domain.exposed.cdl.CDLObjectTypes;
import com.latticeengines.domain.exposed.graph.IdToDisplayNameTranslator;
import com.latticeengines.domain.exposed.graph.VertexType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngine.ScoreType;

@Component
public class CDLIdToDisplayNameTranslator extends IdToDisplayNameTranslator {

    public static final String RATING_ATTRIBUTE = "Rating Attribute";

    @Inject
    private SegmentEntityMgr segmentEntityMgr;

    @Inject
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    @Inject
    private PlayEntityMgr playEntityMgr;

    @Inject
    private RatingDisplayMetadataStore ratingDisplayMetadataStore;

    @Inject
    RatingAttributeNameParser ratingAttributeNameParser;

    @Override
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
            Pair<ScoreType, String> pair = ratingAttributeNameParser.parseTypeNMoelId(type, objId);

            RatingEngine model = ratingEngineEntityMgr.findById(pair.getRight());
            if (model != null) {
                displayName = model.getDisplayName();
            }

            String displayNameSuffix = "";
            if (pair.getLeft() != ScoreType.Rating) {
                displayNameSuffix = " "
                        + ratingDisplayMetadataStore.getSecondaryDisplayName("_" + pair.getLeft().name());
            }
            displayName += displayNameSuffix;
        }
        return displayName;
    }

    @Override
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

    @Override
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
