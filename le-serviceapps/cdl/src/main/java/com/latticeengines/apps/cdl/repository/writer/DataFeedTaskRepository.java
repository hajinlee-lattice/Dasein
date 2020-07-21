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

    @Query("SELECT dft.sourceId, dft.sourceDisplayName, dft.relativePath, dft.s3ImportStatus, dft.pid " +
            "FROM DataFeedTask AS dft JOIN dft.importSystem AS s WHERE s.pid = ?1 AND dft.deleted != True")
    List<Object[]> findSourceInfoBySystemPid(Long systemPid);

    @Query("SELECT dft.sourceId, dft.sourceDisplayName, dft.relativePath, dft.s3ImportStatus, dft.pid " +
            "FROM DataFeedTask AS dft WHERE dft.sourceId = ?1 AND dft.dataFeed = ?2")
    List<Object[]> findBySourceIdAndDataFeed(String sourceId, DataFeed dataFeed);

    @Query("SELECT dft.source, dft.entity, dft.feedType, dft.subType, dft.s3ImportStatus,"
            + " dft.lastUpdated, dft.uniqueId, dft.templateDisplayName " +
            "FROM DataFeedTask dft " +
            "INNER JOIN dft.dataFeed df " +
            "INNER JOIN df.tenant t WHERE dft.source = ?1 AND t.id = ?2")
    List<Object[]> findSummaryBySource(String source, String customerSpace);

    @Query("SELECT dft.source, dft.entity, dft.feedType, dft.subType, dft.s3ImportStatus,"
            + " dft.lastUpdated, dft.uniqueId, dft.templateDisplayName " +
            "FROM DataFeedTask dft " +
            "INNER JOIN dft.dataFeed df " +
            "INNER JOIN df.tenant t WHERE t.id = ?1")
    List<Object[]> findSummaryByDataFeed(String customerSpace);

    @Query("SELECT count(dft) FROM DataFeedTask dft " +
            "INNER JOIN dft.dataFeed df " +
            "INNER JOIN df.tenant t WHERE dft.source = ?1 AND dft.feedType=?2 AND t.id = ?3")
    int countBySourceAndFeedType(String source, String feedType, String customerSpace);

}
