package com.latticeengines.datacloud.etl.service;

import java.util.List;

import com.latticeengines.datacloud.core.source.Source;


public interface SourceService {

    Source findBySourceName(String sourceName);

    List<Source> getSources();

    Source createSource(String sourceName);

    Source findOrCreateSource(String sourceName);

    boolean deleteSource(Source source);
}
