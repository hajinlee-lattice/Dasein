package com.latticeengines.yarn.exposed.service.impl;

import java.io.File;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.ProcessResult;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.Classifier;
import com.latticeengines.yarn.exposed.client.AppMasterProperty;
import com.latticeengines.yarn.exposed.client.ContainerProperty;
import com.latticeengines.yarn.exposed.runtime.python.PythonContainerProperty;
import com.latticeengines.yarn.exposed.service.JobNameService;

@Component
public class JobServiceHelper {

    protected static final Logger log = LoggerFactory.getLogger(JobServiceHelper.class);

    @Autowired
    private JobNameService jobNameService;

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${dataplatform.yarn.job.basedir}")
    private String hdfsJobBaseDir;

    protected String appName(Properties appMasterProperties, String clientName) {
        if (appMasterProperties.containsKey(AppMasterProperty.APP_NAME.name())) {
            return appMasterProperties.getProperty(AppMasterProperty.APP_NAME.name());
        } else if (appMasterProperties.containsKey(AppMasterProperty.APP_NAME_SUFFIX.name())) {
            return jobNameService.createJobName(appMasterProperties.getProperty(AppMasterProperty.CUSTOMER.name()),
                    clientName) + JobNameServiceImpl.JOBNAME_DELIMITER
                    + appMasterProperties.getProperty(AppMasterProperty.APP_NAME_SUFFIX.name());
        } else {
            return jobNameService.createJobName(appMasterProperties.getProperty(AppMasterProperty.CUSTOMER.name()),
                    clientName);
        }
    }

    public String createJobDir() {
        String dir = UUID.randomUUID().toString();
        try {
            HdfsUtils.mkdir(yarnConfiguration, hdfsJobBaseDir + "/" + dir);
            new File("./" + dir).mkdir();
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00000, e, new String[] { dir });
        }
        return dir;
    }

    public void writeMetadataJson(Properties containerProperties) {
        // copy the metadata.json file to HDFS data directory
        String jobType = containerProperties.getProperty(ContainerProperty.JOB_TYPE.name());
        if (jobType != null) {
            String metadata = containerProperties.getProperty(PythonContainerProperty.METADATA_CONTENTS.name());
            Classifier classifier = JsonUtils.deserialize(metadata, Classifier.class);
            String hdfsDir = classifier.getDataDiagnosticsPath();
            hdfsDir = hdfsDir.substring(0, hdfsDir.lastIndexOf('/') + 1);
            String localDir = containerProperties.getProperty(PythonContainerProperty.METADATA.name());
            String random = "-" + UUID.randomUUID().toString();
            String metaDataFileName = "metadata-" + jobType + random + ".json";
            try {
                HdfsUtils.copyLocalToHdfs(yarnConfiguration, localDir, hdfsDir + metaDataFileName);
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_00000, e, new String[] { hdfsDir });
            }
        }
    }

    public String getJobDir(Properties containerProperties) {
        return hdfsJobBaseDir + "/" + containerProperties.getProperty(ContainerProperty.JOBDIR.name());
    }

    public void executePythonCommand(Map<String, String> envs, String conda, File workSpace, String app) {
        try {
            ProcessExecutor executor = new ProcessExecutor().directory(workSpace);
            executor = executor.environment(envs) //
                    .command("bash", "pythonlauncher.sh", conda, app).redirectOutput(System.out);
            ProcessResult result = executor.execute();
            int exit = result.getExitValue();
            log.info("Exit code of python command: " + exit);
            if (exit != 0) {
                throw new RuntimeException("Python program did not exit with code 0, please check error above.");
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute python command.", e);
        }
    }

}
