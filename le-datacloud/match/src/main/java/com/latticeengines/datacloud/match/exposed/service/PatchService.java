package com.latticeengines.datacloud.match.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.match.LookupUpdateRequest;
import com.latticeengines.domain.exposed.datacloud.match.LookupUpdateResponse;

public interface PatchService {

    LookupUpdateResponse patch(List<LookupUpdateRequest> updateRequests);

}
