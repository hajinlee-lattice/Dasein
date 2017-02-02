package com.latticeengines.datacloud.match.exposed.service;

import com.latticeengines.domain.exposed.datacloud.match.LookupUpdateRequest;

public interface PatchService {

    void patch(LookupUpdateRequest updateRequest);

}
