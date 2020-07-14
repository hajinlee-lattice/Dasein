package com.latticeengines.datacloud.match.service;

import java.util.Collection;
import java.util.List;

import com.latticeengines.datacloud.match.service.impl.DirectPlusEnrichRequest;
import com.latticeengines.domain.exposed.datacloud.match.PrimeAccount;

public interface DirectPlusEnrichService {

    List<PrimeAccount> fetch(Collection<DirectPlusEnrichRequest> requests);

}
