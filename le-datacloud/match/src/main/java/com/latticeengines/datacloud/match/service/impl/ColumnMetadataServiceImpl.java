package com.latticeengines.datacloud.match.service.impl;

import javax.annotation.Resource;

import com.latticeengines.common.exposed.util.AvroUtils;
import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn;
import com.latticeengines.domain.exposed.util.MatchTypeUtil;

@Component("columnMetadataService")
public class ColumnMetadataServiceImpl extends BaseColumnMetadataServiceImpl<ExternalColumn> {

    @Resource(name = "externalColumnService")
    private MetadataColumnService<ExternalColumn> externalColumnService;

    @Override
    public boolean accept(String version) {
        return MatchTypeUtil.isValidForRTSBasedMatch(version);
    }

    @Override
    protected MetadataColumnService<ExternalColumn> getMetadataColumnService() {
        return externalColumnService;
    }
}
