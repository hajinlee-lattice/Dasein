package com.latticeengines.propdata.service.db.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.sqoop.LedpSqoop;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.service.JobNameService;
import com.latticeengines.dataplatform.service.impl.JobServiceImpl;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.propdata.service.db.PropDataJobService;

@Component("propDataJobService")
public class PropDataJobServiceImpl extends JobServiceImpl implements
		PropDataJobService {

	@Autowired
	private JobNameService jobNameService;

	@Override
	public ApplicationId importData(String table, String targetDir,
			String queue, String customer, String splitCols, String jdbcUrl) {
		int numDefaultMappers = hadoopConfiguration.getInt(
				"mapreduce.map.cpu.vcores", 4);
		return importData(table, targetDir, queue, customer, splitCols, 1,
				jdbcUrl);
	}

	@Override
	public ApplicationId importData(String table, String targetDir,
			String queue, String customer, String splitCols, int numMappers,
			String jdbcUrl) {

		final String jobName = jobNameService.createJobName(customer,
				"propdata-import");

		importSync(table, targetDir, queue, jobName, splitCols, numMappers,
				jdbcUrl);

		return getApplicationId(jobName);
	}

	private ApplicationId getApplicationId(final String jobName) {
		int tries = 0;
		ApplicationId appId = null;
		while (tries < MAX_TRIES) {
			try {
				Thread.sleep(APP_WAIT_TIME);
			} catch (InterruptedException e) {
				log.warn("Thread.sleep interrupted.", e);
			}
			appId = getAppIdFromName(jobName);
			if (appId != null) {
				return appId;
			}
			tries++;
		}

		return appId;

	}

	private void importSync(final String table, final String targetDir,
			final String queue, final String jobName, final String splitCols,
			final int numMappers, String jdbcUrl) {
		LedpSqoop.runTool(new String[] { //
				"import", //
						"-Dmapred.job.queue.name=" + queue, //
						"--connect", //
						jdbcUrl, //
						"--m", //
						Integer.toString(numMappers), //
						"--table", //
						table, //
						"--as-avrodatafile", //
						"--compress", //
						"--mapreduce-job-name", //
						jobName, //
						"--split-by", //
						splitCols, //
						"--target-dir", //
						targetDir }, new Configuration(yarnConfiguration));

	}

	@Override
	public JobStatus getJobStatus(String applicationId) {
		return null;
	}

	@Override
	public ApplicationId exportData(String table, String sourceDir,
			String queue, String customer, String splitCols, String jdbcUrl) {

		int numDefaultMappers = hadoopConfiguration.getInt(
				"mapreduce.map.cpu.vcores", 4);
		return exportData(table, sourceDir, queue, customer, splitCols,
				numDefaultMappers, jdbcUrl);
	}

	@Override
	public ApplicationId exportData(String table, String sourceDir,
			String queue, String customer, String splitCols, int numMappers,
			String jdbcUrl) {

		final String jobName = jobNameService.createJobName(customer,
				"propdata-export");

		exportSync(table, sourceDir, queue, jobName, splitCols, numMappers,
				jdbcUrl);

		return getApplicationId(jobName);
	}

	private void exportSync(final String table, final String sourceDir,
			final String queue, final String jobName, final String splitCols,
			final int numMappers, String jdbcUrl) {
		LedpSqoop.runTool(new String[] { //
				"export", //
						"-Dmapred.job.queue.name=" + queue, //
						"--connect", //
						jdbcUrl, //
						"--m", //
						Integer.toString(numMappers), //
						"--table", //
						table, //
						"--direct", "--mapreduce-job-name", //
						jobName, //
						"--export-dir", //
						sourceDir }, new Configuration(yarnConfiguration));

	}

}
