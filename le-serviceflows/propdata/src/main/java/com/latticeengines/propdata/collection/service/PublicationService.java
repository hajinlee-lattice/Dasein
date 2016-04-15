package com.latticeengines.propdata.collection.service;

import com.latticeengines.propdata.core.source.Source;

public interface PublicationService {

    void scan();

    void publish(Source source, String creator);

}
