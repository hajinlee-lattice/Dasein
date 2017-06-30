package com.latticeengines.dante.service;

import java.util.List;

import com.latticeengines.domain.exposed.dante.DantePreviewResources;
import com.latticeengines.domain.exposed.dante.DanteTalkingPoint;

public interface TalkingPointService {

    String createOrUpdate(List<DanteTalkingPoint> dtp);

    DanteTalkingPoint findByExternalID(String externalID);

    List<DanteTalkingPoint> findAllByPlayID(String playExternalID);

    void delete(DanteTalkingPoint dtp);

    DantePreviewResources getPreviewResources(String customerSpace);
}
