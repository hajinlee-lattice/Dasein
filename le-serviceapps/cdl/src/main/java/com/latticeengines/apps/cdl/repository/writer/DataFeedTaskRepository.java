package com.latticeengines.apps.cdl.repository.writer;

import java.util.List;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;

public interface DataFeedTaskRepository extends BaseJpaRepository<DataFeedTask, Long> {

    DataFeedTask findBySourceAndFeedTypeAndEntityAndDataFeed(String source, String datafeedType, String entity,
                                                             DataFeed dataFeed);

    DataFeedTask findBySourceAndFeedTypeAndDataFeed(String source, String datafeedType, DataFeed dataFeed);

    List<DataFeedTask> findByEntityAndDataFeed(String entity, DataFeed dataFeed);

    List<DataFeedTask> findByUniqueIdIn(List<String> uniqueIds);

    DataFeedTask findByUniqueId(String uniqueId);

    DataFeedTask findByDataFeedAndSourceId(DataFeed dataFeed, String sourceId);

    DataFeedTask findByDataFeedAndTaskUniqueName(DataFeed dataFeed, String taskUniqueName);

    @Transactional
    @Modifying(clearAutomatically = true)
    @Query("UPDATE DataFeedTask dft SET dft.deleted = :deleted WHERE dft.pid = :pid")
    void setDataFeedTaskDeleted(@Param("pid") Long pid, @Param("deleted") Boolean deleted);

}
