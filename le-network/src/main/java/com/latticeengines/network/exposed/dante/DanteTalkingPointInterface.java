package com.latticeengines.network.exposed.dante;

import java.util.List;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dante.DantePreviewResources;
import com.latticeengines.domain.exposed.dante.DanteTalkingPoint;

public interface DanteTalkingPointInterface {

    ResponseDocument<?> createOrUpdate(List<DanteTalkingPoint> talkingPoints);

    ResponseDocument<?> delete(String externalID);

    ResponseDocument<List<DanteTalkingPoint>> findAllByPlayID(String playID);

    ResponseDocument<DanteTalkingPoint> findByExternalID(String externalID);

    ResponseDocument<DantePreviewResources> getPreviewResources(String customerSpace);

    // ResponseDocument<TalkingPointPreview> getTalkingPointPreview(String
    // PlayID);
}
