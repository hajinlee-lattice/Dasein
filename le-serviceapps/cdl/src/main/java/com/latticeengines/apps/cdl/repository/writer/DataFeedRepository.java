package com.latticeengines.apps.cdl.repository.writer;

import java.util.List;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.security.TenantStatus;

public interface DataFeedRepository extends BaseJpaRepository<DataFeed, Long> {

    DataFeed findByName(String datafeedName);

    DataFeed findByDataCollection(DataCollection dataCollection);

    @Query("select df from DataFeed df where df.tenant.status = :status")
    List<DataFeed> getDataFeedsByTenantStatus(@Param("status") TenantStatus status);
}
