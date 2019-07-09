package com.latticeengines.apps.cdl.repository.writer;

import java.util.List;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.SchedulingGroup;
import com.latticeengines.domain.exposed.security.TenantStatus;

public interface DataFeedRepository extends BaseJpaRepository<DataFeed, Long> {

    DataFeed findByName(String datafeedName);

    DataFeed findByDataCollection(DataCollection dataCollection);

    @Query("select df from DataFeed df where df.tenant.status = :status and df.tenant.uiVersion = :version")
    List<DataFeed> getDataFeedsByTenantStatus(@Param("status") TenantStatus status, @Param("version") String version);

    @Query("select df from DataFeed df where df.tenant.status = :status and df.tenant.uiVersion = :version and df" +
            ".schedulingGroup = :schedulingGroup")
    List<DataFeed> getDataFeedsByTenantStatusAndSchedulingType(@Param("status") TenantStatus status,
                                                               @Param("version") String version, @Param(
                                                                       "schedulingGroup") SchedulingGroup schedulingGroup);
}
