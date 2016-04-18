package com.latticeengines.propdata.engine.publication.service;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.manage.Publication;
import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;

public interface PublicationProgressService {

    PublicationProgress kickoffNewProgress(Publication publication, String creator);

    PublicationProgress publishVersion(Publication publication, String version, String creator);

    PublicationProgressUpdater update(PublicationProgress progress);

    List<PublicationProgress> scanNonTerminalProgresses();

}
