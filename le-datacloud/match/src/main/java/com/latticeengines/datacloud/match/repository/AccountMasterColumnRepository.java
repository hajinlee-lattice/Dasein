package com.latticeengines.datacloud.match.repository;

import java.util.List;

import org.springframework.data.domain.Pageable;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;

public interface AccountMasterColumnRepository extends BaseJpaRepository<AccountMasterColumn, Long> {

    long countByDataCloudVersion(String dataCloudVersion);

    List<AccountMasterColumn> findByDataCloudVersion(String dataCloudVersion);

    List<AccountMasterColumn> findByDataCloudVersion(String dataCloudVersion, Pageable pageable);

}
