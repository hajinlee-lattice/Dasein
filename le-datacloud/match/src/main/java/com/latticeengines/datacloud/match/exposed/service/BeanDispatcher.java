package com.latticeengines.datacloud.match.exposed.service;

import com.latticeengines.datacloud.match.service.impl.MatchContext;

public interface BeanDispatcher {

    DbHelper getDbHelper(String dataCloudVersion);

    DbHelper getDbHelper(MatchContext context);

    ColumnSelectionService getColumnSelectionService(String dataCloudVersion);

    ColumnSelectionService getColumnSelectionService(MatchContext context);

    ColumnMetadataService getColumnMetadataService(String dataCloudVersion);

    MetadataColumnService getMetadataColumnService(String dataCloudVersion);

    MetadataColumnService getMetadataColumnService(MatchContext context);

}
