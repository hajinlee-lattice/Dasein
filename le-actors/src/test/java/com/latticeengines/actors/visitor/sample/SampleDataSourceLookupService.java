package com.latticeengines.actors.visitor.sample;

import com.latticeengines.actors.exposed.traveler.Response;

public interface SampleDataSourceLookupService {

    void asyncLookup(String lookupId, Object request, String returnAddress);

    Response syncLookup(Object request);

}
