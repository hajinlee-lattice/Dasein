package com.latticeengines.propdata.core.service;

import java.util.List;

import com.latticeengines.propdata.core.source.Source;

public interface SourceService {

    Source findBySourceName(String sourceName);

    List<Source> getSources();

}
