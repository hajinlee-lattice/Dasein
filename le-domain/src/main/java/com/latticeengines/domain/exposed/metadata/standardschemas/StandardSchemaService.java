package com.latticeengines.domain.exposed.metadata.standardschemas;

import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.Table;

public interface StandardSchemaService {

    /**
     *
     * @param systemType Import System Type
     * @param feedType FeedType from DataFeedTask should be "AccountData" / "{SystemName}_ContactData" etc..
     * @param enableEntityMatch FeatureFlag for entityMatch
     * @return standard schema table.
     *
     * This interface should have multiple implementations, SchemaRepository is one of them. Will sync with Jonathan
     * to see if we can use spec files.
     */
    Table getStandardSchema(S3ImportSystem.SystemType systemType, String feedType, boolean enableEntityMatch);
}
