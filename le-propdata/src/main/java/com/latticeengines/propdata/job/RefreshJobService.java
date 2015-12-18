package com.latticeengines.propdata.job;

import java.util.Date;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.propdata.collection.util.DateRange;

public interface RefreshJobService {

    void archivePeriod(DateRange period, boolean downloadOnly);

    void setJobSubmitter(String jobSubmitter);

    void retryJob(ArchiveProgress progress);

    void pivotData(Date pivotDate, String baseSourceVersion);
}
