package com.latticeengines.propdata.collection.job;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.propdata.collection.util.DateRange;

public interface ArchiveJobService {

    void archivePeriod(DateRange period);

    void setJobSubmitter(String jobSubmitter);

    void setAutowiredArchiveService();

    void retryJob(ArchiveProgress progress);

}
