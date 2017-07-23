package com.latticeengines.network.exposed.dante;

import java.util.List;

import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.dante.DantePreviewResources;
import com.latticeengines.domain.exposed.dante.TalkingPointPreview;
import com.latticeengines.domain.exposed.pls.TalkingPointDTO;

public interface TalkingPointInterface {

    SimpleBooleanResponse createOrUpdate(List<TalkingPointDTO> talkingPoints, String customerSpace);

    SimpleBooleanResponse delete(String name);

    List<TalkingPointDTO> findAllByPlayName(String playName);

    TalkingPointDTO findByName(String name);

    TalkingPointPreview getTalkingPointPreview(String playName, String customerSpace);

    DantePreviewResources getPreviewResources(String customerSpace);

    SimpleBooleanResponse publish(String playName, String customerSpace);
}
