package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.DantePreviewResources;
import com.latticeengines.domain.exposed.cdl.TalkingPointPreview;
import com.latticeengines.domain.exposed.cdl.TalkingPointDTO;
import com.latticeengines.domain.exposed.query.AttributeLookup;

public interface TalkingPointService {

    List<TalkingPointDTO> createOrUpdate(List<TalkingPointDTO> dtp, String customerSpace);

    TalkingPointDTO findByName(String name);

    List<TalkingPointDTO> findAllByPlayName(String playName, boolean publishedOnly);

    void delete(String name);

    DantePreviewResources getPreviewResources(String customerSpace);

    void publish(String playName, String customerSpace);

    TalkingPointPreview getPreview(String playName, String customerSpace);

    List<TalkingPointDTO> revertToLastPublished(String playName, String customerSpace);

    List<AttributeLookup> getAttributesInTalkingPointOfPlay(String playName);
}
