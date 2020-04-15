package com.latticeengines.domain.exposed.util;

import java.util.Date;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.EntityType;

public final class DataFeedTaskUtils {

    protected DataFeedTaskUtils() {
        throw new UnsupportedOperationException();
    }

    public static DataFeedTask generateDataFeedTask(String feedType, String source, Table templateTable,
                                                    EntityType entityType, String templateDisplayName) {
        DataFeedTask dataFeedTask = new DataFeedTask();
        dataFeedTask.setUniqueId(NamingUtils.uuid("DataFeedTask"));
        dataFeedTask.setImportTemplate(templateTable);
        dataFeedTask.setStatus(DataFeedTask.Status.Active);
        dataFeedTask.setEntity(entityType.getEntity().name());
        dataFeedTask.setFeedType(feedType);
        dataFeedTask.setSource(source);
        dataFeedTask.setActiveJob("Not specified");
        dataFeedTask.setSourceConfig("Not specified");
        dataFeedTask.setStartTime(new Date());
        dataFeedTask.setLastImported(new Date(0L));
        dataFeedTask.setLastUpdated(new Date());
        dataFeedTask.setDeleted(Boolean.FALSE);
        dataFeedTask.setSubType(entityType.getSubType());
        dataFeedTask.setTemplateDisplayName(templateDisplayName);
        return dataFeedTask;
    }

    public static DataFeedTask generateDataFeedTask(String feedType, String source, S3ImportSystem importSystem,
                                                    Table templateTable, EntityType entityType, String relativePath,
                                                    String displayName, String sourceId) {
        DataFeedTask dataFeedTask = new DataFeedTask();
        dataFeedTask.setUniqueId(NamingUtils.uuid("DataFeedTask"));
        dataFeedTask.setImportTemplate(templateTable);
        dataFeedTask.setStatus(DataFeedTask.Status.Active);
        dataFeedTask.setEntity(entityType.getEntity().name());
        dataFeedTask.setFeedType(feedType);
        dataFeedTask.setSource(source);
        dataFeedTask.setActiveJob("Not specified");
        dataFeedTask.setSourceConfig("Not specified");
        dataFeedTask.setStartTime(new Date());
        dataFeedTask.setLastImported(new Date(0L));
        dataFeedTask.setLastUpdated(new Date());
        dataFeedTask.setDeleted(Boolean.FALSE);
        dataFeedTask.setSubType(entityType.getSubType());
        dataFeedTask.setTemplateDisplayName(dataFeedTask.getFeedType());
        dataFeedTask.setImportSystem(importSystem);
        dataFeedTask.setRelativePath(relativePath);
        dataFeedTask.setSourceDisplayName(displayName);
        dataFeedTask.setSourceId(sourceId);
        return dataFeedTask;
    }

}
