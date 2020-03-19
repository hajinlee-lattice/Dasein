package com.latticeengines.pls.service.dcp;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.SourceRequest;

public interface SourceService {

    Source createSource(SourceRequest sourceRequest);

    Source getSource(String sourceId);

    List<Source> getSourceList(String projectId);

    Boolean deleteSource(String sourceId);

}
