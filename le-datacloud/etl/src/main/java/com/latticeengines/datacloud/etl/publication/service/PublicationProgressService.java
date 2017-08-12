package com.latticeengines.datacloud.etl.publication.service;

import java.io.IOException;
import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.Publication;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationDestination;

public interface PublicationProgressService {

    PublicationProgress kickoffNewProgress(Publication publication, String creator) throws IOException;

    PublicationProgress publishVersion(Publication publication, String version, String creator);

    PublicationProgress publishVersion(Publication publication, PublicationDestination destination, String version,
            String creator);

    PublicationProgressUpdater update(PublicationProgress progress);

    List<PublicationProgress> scanNonTerminalProgresses();

}
