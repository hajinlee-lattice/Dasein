package com.latticeengines.datacloud.match.actors.visitor;

import com.latticeengines.actors.exposed.traveler.Response;

public interface DataSourceLookupService {

    void asyncLookup(String lookupId, Object request, String returnAddress);

    Response syncLookup(Object request);

}
