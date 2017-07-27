package com.latticeengines.network.exposed.dante;

import java.util.List;

import com.latticeengines.domain.exposed.dante.DantePreviewResources;
import com.latticeengines.domain.exposed.dante.TalkingPointPreview;
import com.latticeengines.domain.exposed.pls.TalkingPointDTO;

public interface TalkingPointInterface {

    List<TalkingPointDTO> createOrUpdate(List<TalkingPointDTO> talkingPoints, String customerSpace);

    void delete(String name);

    List<TalkingPointDTO> findAllByPlayName(String playName);

    TalkingPointDTO findByName(String name);

    TalkingPointPreview getTalkingPointPreview(String playName, String customerSpace);

    DantePreviewResources getPreviewResources(String customerSpace);

    void publish(String playName, String customerSpace);
}
