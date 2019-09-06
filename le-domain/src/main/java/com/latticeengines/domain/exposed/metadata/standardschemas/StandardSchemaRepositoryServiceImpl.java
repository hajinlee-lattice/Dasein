package com.latticeengines.domain.exposed.metadata.standardschemas;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.query.EntityTypeUtils;

@Component("standardSchemaRepositoryService")
public class StandardSchemaRepositoryServiceImpl implements StandardSchemaService {

    @Override
    public Table getStandardSchema(S3ImportSystem.SystemType systemType, String feedType, boolean enableEntityMatch) {
        if (StringUtils.isBlank(feedType)) {
            return null;
        }
        EntityType entityType = EntityTypeUtils.matchFeedType(feedType);
        if (entityType == null) {
            return null;
        }
        return SchemaRepository.instance().getSchema(systemType, entityType, enableEntityMatch);
    }
}
