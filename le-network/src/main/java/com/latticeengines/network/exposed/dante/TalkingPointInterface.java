package com.latticeengines.network.exposed.dante;

import java.util.List;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dante.DantePreviewResources;
import com.latticeengines.domain.exposed.dante.TalkingPointPreview;
import com.latticeengines.domain.exposed.pls.TalkingPointDTO;

public interface TalkingPointInterface {

    ResponseDocument<?> createOrUpdate(List<TalkingPointDTO> talkingPoints, String customerSpace);

    ResponseDocument<?> delete(String name);

    ResponseDocument<List<TalkingPointDTO>> findAllByPlayName(String playName);

    ResponseDocument<TalkingPointDTO> findByName(String name);

    ResponseDocument<TalkingPointPreview> getTalkingPointPreview(String playName, String customerSpace);

    ResponseDocument<DantePreviewResources> getPreviewResources(String customerSpace);

    ResponseDocument<?> publish(String playName, String customerSpace);
}
