package com.latticeengines.actors.visitor.sample;

import com.latticeengines.actors.exposed.traveler.Response;

public interface SampleDataSourceLookupService {

    void asyncLookup(String lookupId, Object inputData, String returnAddress, Object system);

    Response syncLookup(Object inputData);

}
