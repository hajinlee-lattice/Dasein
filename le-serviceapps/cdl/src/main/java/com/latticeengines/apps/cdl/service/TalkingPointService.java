package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.DantePreviewResources;
import com.latticeengines.domain.exposed.cdl.TalkingPointDTO;
import com.latticeengines.domain.exposed.cdl.TalkingPointPreview;
import com.latticeengines.domain.exposed.query.AttributeLookup;

public interface TalkingPointService {

    List<TalkingPointDTO> createOrUpdate(List<TalkingPointDTO> dtp);

    TalkingPointDTO findByName(String name);

    List<TalkingPointDTO> findAllByPlayName(String playName, boolean publishedOnly);

    void delete(String name);

    DantePreviewResources getPreviewResources();

    void publish(String playName);

    TalkingPointPreview getPreview(String playName);

    List<TalkingPointDTO> revertToLastPublished(String playName);

    List<AttributeLookup> getAttributesInTalkingPointOfPlay(String playName);

    List<TalkingPointDTO> findAllPublishedByTenant(String customerSpace);
}
