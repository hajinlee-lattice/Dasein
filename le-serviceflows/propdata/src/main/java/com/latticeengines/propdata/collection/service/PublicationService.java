package com.latticeengines.propdata.collection.service;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.manage.Publication;
import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;
import com.latticeengines.propdata.core.source.Source;

public interface PublicationService {

    List<PublicationProgress> publishLatest(Source source, String creator);
    PublicationProgress publishVersion(Publication publication, String version, String creator);
    PublicationProgressUpdater update(PublicationProgress progress);
    List<PublicationProgress> scanNonTerminalProgresses();

}
