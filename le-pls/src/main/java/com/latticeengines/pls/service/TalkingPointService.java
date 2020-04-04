package com.latticeengines.pls.service;

import java.util.List;

import org.springframework.web.bind.annotation.PathVariable;

import com.latticeengines.domain.exposed.cdl.DantePreviewResources;
import com.latticeengines.domain.exposed.cdl.TalkingPointDTO;
import com.latticeengines.domain.exposed.cdl.TalkingPointNotionAttributes;
import com.latticeengines.domain.exposed.cdl.TalkingPointPreview;

public interface TalkingPointService {
    List<TalkingPointDTO> createOrUpdate(List<TalkingPointDTO> talkingPoints);

    TalkingPointDTO findByName(String talkingPointName);

    List<TalkingPointDTO> findAllByPlayName(String playName);

    DantePreviewResources getPreviewResources();

    TalkingPointPreview preview(String playName);

    void publish(String playName);

    List<TalkingPointDTO> revert(String playName);

    void delete(@PathVariable String talkingPointName);

    TalkingPointNotionAttributes getAttributesByNotions(List<String> notions);
}
