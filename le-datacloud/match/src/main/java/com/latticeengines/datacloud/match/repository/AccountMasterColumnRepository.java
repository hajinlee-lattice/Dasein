package com.latticeengines.datacloud.match.repository;

import java.util.List;

import javax.persistence.QueryHint;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;

public interface AccountMasterColumnRepository extends BaseJpaRepository<AccountMasterColumn, Long> {

    Long countByDataCloudVersion(String dataCloudVersion);

    @Query("select count(1) from AccountMasterColumn a where a.dataCloudVersion = ?1")
    Long numAttrsInVersion(String dataCloudVersion);

    List<AccountMasterColumn> findByDataCloudVersion(String dataCloudVersion);

    @QueryHints(value = { @QueryHint(name = "javax.persistence.query.timeout", value = "90000") })
    List<AccountMasterColumn> findByDataCloudVersion(String dataCloudVersion, Pageable pageable);
}
