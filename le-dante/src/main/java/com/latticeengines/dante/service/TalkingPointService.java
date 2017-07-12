package com.latticeengines.dante.service;

import java.util.List;

import com.latticeengines.domain.exposed.dante.DantePreviewResources;
import com.latticeengines.domain.exposed.pls.TalkingPoint;

public interface TalkingPointService {

    String createOrUpdate(List<TalkingPoint> dtp);

    TalkingPoint findByName(String name);

    List<TalkingPoint> findAllByPlayId(Long playId);

    void delete(TalkingPoint tp);

    DantePreviewResources getPreviewResources(String customerSpace);

    void publish(Long playId);
}
