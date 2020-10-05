package com.latticeengines.datacloud.match.repository.reader;

import java.util.Date;
import java.util.List;

import com.latticeengines.datacloud.match.repository.VboUsageReportRepository;
import com.latticeengines.domain.exposed.datacloud.manage.VboUsageReport;

public interface VboUsageReportReaderRepository extends VboUsageReportRepository {

    List<VboUsageReport> findAllByQuietPeriodEndAfter(Date threshold);

    List<VboUsageReport> findAllBySubmittedAtIsNull();

}

