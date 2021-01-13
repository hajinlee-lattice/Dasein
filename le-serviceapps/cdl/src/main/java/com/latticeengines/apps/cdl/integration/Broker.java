package com.latticeengines.apps.cdl.integration;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.IngestionScheduler;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;

public interface Broker {

    void pause();

    void start();

    List<String> listDocumentTypes();

    List<ColumnMetadata> describeDocumentType(String documentType);

    void schedule(IngestionScheduler scheduler);
}
