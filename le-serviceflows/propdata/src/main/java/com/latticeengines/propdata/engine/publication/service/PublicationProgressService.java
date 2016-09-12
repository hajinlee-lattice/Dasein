package com.latticeengines.propdata.engine.publication.service;

import java.io.IOException;
import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.Publication;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;

public interface PublicationProgressService {

    PublicationProgress kickoffNewProgress(Publication publication, String creator) throws IOException;

    PublicationProgress publishVersion(Publication publication, String version, String creator);

    PublicationProgressUpdater update(PublicationProgress progress);

    List<PublicationProgress> scanNonTerminalProgresses();

}
