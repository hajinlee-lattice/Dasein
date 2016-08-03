package com.latticeengines.propdata.match.service.impl;

import javax.annotation.Resource;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.propdata.match.service.MetadataColumnService;

@Component("columnMetadataService")
public class ColumnMetadataServiceImpl extends BaseColumnMetadataServiceImpl<ExternalColumn> {

    @Resource(name = "externalColumnService")
    private MetadataColumnService<ExternalColumn> externalColumnService;

    @Override
    protected MetadataColumnService<ExternalColumn> getMetadataColumnService() {
        return externalColumnService;
    }
}
