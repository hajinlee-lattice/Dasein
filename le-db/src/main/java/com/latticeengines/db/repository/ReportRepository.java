package com.latticeengines.db.repository;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.workflow.Report;

public interface ReportRepository extends BaseJpaRepository<Report, Long> {

    Report findByName(String name);

}
