package com.latticeengines.dante.service;

import java.util.List;

import com.latticeengines.domain.exposed.dante.DantePreviewResources;
import com.latticeengines.domain.exposed.dante.TalkingPointPreview;
import com.latticeengines.domain.exposed.multitenant.TalkingPointDTO;

public interface TalkingPointService {

    List<TalkingPointDTO> createOrUpdate(List<TalkingPointDTO> dtp, String customerSpace);

    TalkingPointDTO findByName(String name);

    List<TalkingPointDTO> findAllByPlayName(String playName);

    void delete(String name);

    DantePreviewResources getPreviewResources(String customerSpace);

    void publish(String playName, String customerSpace);

    TalkingPointPreview getPreview(String playName, String customerSpace);

    List<TalkingPointDTO> revertToLastPublished(String playName, String customerSpace);
}
