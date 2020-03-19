package com.latticeengines.proxy.exposed.dcp;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.SourceRequest;

public interface SourceProxy {

    Source createSource(String customerSpace, SourceRequest sourceRequest);

    Source getSource(String customerSpace, String sourceId);

    List<Source> getSourceList(String customerSpace, String projectId);
}
