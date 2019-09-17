package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.cdl.SimpleTemplateMetadata;

public interface DataFeedTaskTemplateService {

    /**
     * @param customerSpace
     * @param simpleTemplateMetadata Template description.
     * @return true if success.
     */
    boolean setupWebVisitTemplate(String customerSpace, SimpleTemplateMetadata simpleTemplateMetadata);
}
