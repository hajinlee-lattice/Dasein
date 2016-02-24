package com.latticeengines.quartz.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.quartz.JobHistory;

public interface JobHistoryDao extends BaseDao<JobHistory> {
	List<JobHistory> getJobHistory(String tenantId, String jobName);

	void saveJobHistory(JobHistory jobHistory);

}
