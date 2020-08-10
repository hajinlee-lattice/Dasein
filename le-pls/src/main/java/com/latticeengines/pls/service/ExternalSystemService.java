package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.frontend.ExternalSystemMapping;

public interface ExternalSystemService {

    List<ExternalSystemMapping> getExternalSystemMappings(String entity, String source, String feedType,
                                                          Boolean includeAll);
}
