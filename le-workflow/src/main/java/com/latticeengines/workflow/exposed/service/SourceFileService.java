package com.latticeengines.workflow.exposed.service;

import com.latticeengines.domain.exposed.workflow.SourceFile;

public interface SourceFileService {

    SourceFile findByName(String name);
}
