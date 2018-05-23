package com.latticeengines.apps.lp.repository;

import java.util.List;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.security.Tenant;

public interface ModelSummaryRepository extends BaseJpaRepository<ModelSummary, Long> {

    ModelSummary findById(String modelGuid);

    @Query("select id from ModelSummary")
    List<String> findAllModelSummaryIds();

    List<ModelSummary> findByTenant(Tenant tenant);

    List<ModelSummary> findByStatus(ModelSummaryStatus status);

    @Query("select m from ModelSummary m where m.status != :status")
    List<ModelSummary> findByNotInStatus(@Param("status") ModelSummaryStatus status);

}
