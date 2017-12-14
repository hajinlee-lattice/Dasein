package com.latticeengines.network.exposed.propdata;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.PurgeSource;

public interface PurgeInterface {
    List<PurgeSource> getPurgeSources(String hdfsPod);
}
