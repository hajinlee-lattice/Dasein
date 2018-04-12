package com.latticeengines.datacloudapi.engine.purge.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.PurgeSource;

public interface PurgeService {
    List<PurgeSource> scan(String hdfsPod, boolean debug);

    List<String> scanUnknownSources(String hdfsPod);
}
