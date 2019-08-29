package com.latticeengines.apps.cdl.service;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionArtifact;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;

import reactor.core.publisher.Flux;

@Component("proxyResourceService")
public interface ProxyResourceService {

    void registerAction(Action action, String user);

    Flux<ColumnMetadata> getNewModelingAttrs(String customerSpace, BusinessEntity entity,
                                             DataCollection.Version version);

    Table getTable(String customerSpace, TableRoleInCollection role, DataCollection.Version version);

    DataCollection getDataCollection(String customerSpace);

    DataCollectionArtifact createArtifact(String customerSpace, DataCollectionArtifact artifact, DataCollection.Version version);

    void resetTable(String customerSpace, TableRoleInCollection tableRole);

    Table getTable(String customerSpace, TableRoleInCollection role);

    DataCollectionStatus getOrCreateDataCollectionStatus(String customerSpace, DataCollection.Version version);

    DataCollection.Version getActiveVersion(String customerSpace);

    List<ActivityMetrics> getActivityMetrics();

    boolean hasContact(String customerSpace, DataCollection.Version version);

    void resetImport(String customerSpace);

    DataFeed getDataFeed(String customerSpace);

    DataFeedExecution failExecution(String customerSpace, String initialDataFeedStatus);

    Long lockExecution(String customerSpace, DataFeedExecutionJobType jobType);

    DataFeedExecution getLatestExecution(String customerSpace, DataFeedExecutionJobType jobType);

    Long restartExecution(String customerSpace, DataFeedExecutionJobType jobType);

    void updateDataFeedStatus(String customerSpace, String status);

    DataFeedExecution finishExecution(String customerSpace, String initialDataFeedStatus);

    void updateDataFeedTask(String customerSpace, DataFeedTask dataFeedTask);

    DataFeedTask getDataFeedTask(String customerSpace, String source, String dataFeedType, String entity);

    void createDataFeedTask(String customerSpace, DataFeedTask dataFeedTask);

    DataFeedTask getDataFeedTask(String customerSpace, String taskId);

    DataFeedTask getDataFeedTask(String customerSpace, String source, String dataFeedType);

    List<DataFeedTask> getDataFeedTaskWithSameEntity(String customerSpace, String entity);
}
