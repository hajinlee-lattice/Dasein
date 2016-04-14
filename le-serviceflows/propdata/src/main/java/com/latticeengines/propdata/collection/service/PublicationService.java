package com.latticeengines.propdata.collection.service;

import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;
import com.latticeengines.propdata.core.source.Source;

public interface PublicationService {

    PublicationProgress startNewProgressIfAppropriate(Source source, String creator);
    PublicationProgressUpdater update(PublicationProgress progress);
}
