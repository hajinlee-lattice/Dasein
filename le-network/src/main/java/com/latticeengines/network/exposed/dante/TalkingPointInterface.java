package com.latticeengines.network.exposed.dante;

import java.util.List;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dante.DantePreviewResources;
import com.latticeengines.domain.exposed.pls.TalkingPoint;

public interface TalkingPointInterface {

    ResponseDocument<?> createOrUpdate(List<TalkingPoint> talkingPoints);

    ResponseDocument<?> delete(String externalID);

    ResponseDocument<List<TalkingPoint>> findAllByPlayID(Long playID);

    ResponseDocument<TalkingPoint> findByName(String name);

    ResponseDocument<DantePreviewResources> getPreviewResources(String customerSpace);

    // ResponseDocument<TalkingPointPreview> getTalkingPointPreview(String
    // PlayID);

    ResponseDocument<?> publish(Long playId);
}
