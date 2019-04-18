package com.latticeengines.datacloudapi.engine.purge.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.PurgeSource;

public interface SourcePurger {

    List<PurgeSource> findSourcesToPurge(boolean debug);

}
