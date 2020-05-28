package com.latticeengines.apps.cdl.repository.writer;

import java.util.List;

import org.springframework.data.jpa.repository.Query;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;

public interface DataFeedTaskRepository extends BaseJpaRepository<DataFeedTask, Long> {

    DataFeedTask findBySourceAndFeedTypeAndEntityAndDataFeed(String source, String datafeedType, String entity,
                                                             DataFeed dataFeed);

    DataFeedTask findBySourceAndFeedTypeAndDataFeed(String source, String datafeedType, DataFeed dataFeed);

    List<DataFeedTask> findByEntityAndDataFeed(String entity, DataFeed dataFeed);

    List<DataFeedTask> findByDataFeed(DataFeed dataFeed);

    List<DataFeedTask> findByUniqueIdIn(List<String> uniqueIds);

    DataFeedTask findByUniqueId(String uniqueId);

    DataFeedTask findByDataFeedAndSourceId(DataFeed dataFeed, String sourceId);

    DataFeedTask findByDataFeedAndTaskUniqueName(DataFeed dataFeed, String taskUniqueName);

    @Query("select dft.sourceId, dft.sourceDisplayName, dft.relativePath, dft.s3ImportStatus, dft.pid " +
            "from DataFeedTask as dft join dft.importSystem as s where s.pid = ?1 and dft.deleted != True")
    List<Object[]> findSourceInfoBySystemPid(Long systemPid);

    @Query("select dft.sourceId, dft.sourceDisplayName, dft.relativePath, dft.s3ImportStatus, dft.pid " +
            "from DataFeedTask as dft where dft.sourceId = ?1 and dft.dataFeed = ?2")
    List<Object[]> findBySourceIdAndDataFeed(String sourceId, DataFeed dataFeed);

}
