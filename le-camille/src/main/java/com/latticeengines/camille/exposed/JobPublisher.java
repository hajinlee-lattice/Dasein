package com.latticeengines.camille.exposed;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;


public class JobPublisher {
    private static final Logger log = LoggerFactory.getLogger(JobPublisher.class);

    public JobPublisher(String rootPath)
    {
        setRootPath(rootPath);
    }


    public Path getRequestPath(String jobID)
    {
        if(StringUtils.isBlank(jobID)) {
            jobID = UUID.randomUUID().toString();
        }

        return rootPath.append(requestsFolder).append(jobID);
    }

    public static String decodeBytes(byte[] utf8Bytes)
    {
        return new String(utf8Bytes);
    }

    public static byte[] getBytes(String inputString)
    {
        return inputString.getBytes(StandardCharsets.UTF_8);
    }

    public void registerJob(String jobID, String jobData)
    {
        try {
            initialize();

            Path toCreate = getRequestPath(jobID);
            Camille camille = CamilleEnvironment.getCamille();

            byte[] dataBytes = getBytes(jobData);
            camille.getCuratorClient()
                    .create()
                    .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    .forPath(toCreate.toString(), dataBytes);

            Path tryNumberPath = toCreate.append(tryNumAttribute);
            camille.getCuratorClient()
                    .create()
                    .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    .forPath(tryNumberPath.toString(), getBytes("0"));

        }
        catch (Exception exc)
        {
            throw new LedpException(LedpCode.LEDP_15019, new Object[]{exc.getMessage()});
        }
    }

    public int getTryNum(String jobID)
    {
        try {
            Path tryNumPath = getRequestPath(jobID).append(tryNumAttribute);
            String tryNumString =  decodeBytes(
                    CamilleEnvironment
                            .getCamille()
                            .getCuratorClient()
                            .getData()
                            .forPath(tryNumPath.toString()));

            return Integer.parseInt(tryNumString);
        }
        catch (Exception exc)
        {
            return -1;
        }
    }

    public boolean isOpenJob(String jobID)
    {
        try {
            Camille camille = CamilleEnvironment.getCamille();
            Path jobPath = getRequestPath(jobID);
            Path tryNumPath = jobPath.append(tryNumAttribute);
            Path executionTestPath = jobPath.append(executorIDAttribute);

            if(!camille.exists(jobPath) || !camille.exists(tryNumPath) || camille.exists(executionTestPath))
                return false;

            if(getTryNum(jobID) >= 4)
                return false;

            return true;
        }
        catch (Exception exc)
        {
            log.info("Transient error validating job: " + jobID);
            return false;
        }
    }

    public boolean attemptExecution(IJobExecutor executor, String executorID, String jobID)
    {
        boolean toReturn = false;

        Path jobPath = getRequestPath(jobID);
        Path executorDataPath = jobPath.append(executorIDAttribute);
        Camille camille = CamilleEnvironment.getCamille();

        try {
            if(!isOpenJob(jobID))
                return false;
            String data = new String(
                    camille.getCuratorClient().getData().forPath(jobPath.toString()));

            camille.getCuratorClient()
                    .create()
                    .withMode(CreateMode.EPHEMERAL)
                    .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    .forPath(executorDataPath.toString(), getBytes(executorID));

            int tryNum = getTryNum(jobID) + 1;
            camille.getCuratorClient()
                    .setData()
                    .forPath(jobPath.append(tryNumAttribute).toString(),
                            getBytes(Integer.toString(tryNum)));
            try {
                executor.execute(data);
                toReturn = true;
                camille.delete(jobPath);
            }
            catch (Exception exc)
            {
                log.error("Failed to execute Job: " + jobID, exc);
                if(tryNum >= 3)
                {
                    camille.delete(jobPath);
                    camille.getCuratorClient()
                            .create()
                            .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                            .forPath(rootPath.append(failedJobsFolder).append(jobID).toString(),
                                    getBytes(data));
                }
            }
        }
        catch (Exception exc)
        {
            toReturn = false;
        }
        finally
        {
            try {
                if(camille.exists(executorDataPath))
                    camille.delete(executorDataPath);
            } catch (Exception e) {
                log.debug("Executor data path {} already removed, ignore delete", executor);
            }
        }

        return toReturn;
    }

    protected void initialize()
    {
        if(isInitialized)
        {
            return;
        }

        Path requestsPath = rootPath.append(requestsFolder);
        Camille camille = CamilleEnvironment.getCamille();
        try {
            if(!camille.exists(requestsPath))
            {
                camille.create(requestsPath, ZooDefs.Ids.OPEN_ACL_UNSAFE, true);
            }
        } catch (Exception e) {
            log.debug("Requests path {} already exist, ignore create", requestsPath);
        }
        Path failuresPath = rootPath.append(failedJobsFolder);
        try {
            if(!camille.exists(failuresPath))
            {
                camille.create(failuresPath, ZooDefs.Ids.OPEN_ACL_UNSAFE, true);
            }
        } catch (Exception e) {
            log.debug("Failures path {} already exist, ignore create", requestsPath);
        }
        isInitialized = true;

    }

    public static final String dataAttribute = "Data";
    public static final String statusAttribute = "Status";
    public static final String jobIdAttribute = "JobID";
    public static final String executorIDAttribute = "ExecutorID";
    public static final String exceptionDataAttribute = "ExceptionData";
    public static final String tryNumAttribute = "TryNum";

    public static final String requestsFolder = "Requests";
    public static final String failedJobsFolder = "Failures";

    @FunctionalInterface
    public interface IJobExecutor
    {
        void execute(String nodeData);
    }

    public Path getRootPath()
    {
        return rootPath;
    }

    public void setRootPath(String rootPath) {
        this.rootPath = new Path(rootPath);
    }

    public int getMaxNumTries()
    {
        return maxNumTries;
    }

    public void setMaxNumTries(int maxNumTries)
    {
        this.maxNumTries = maxNumTries;
    }

    private Path rootPath;
    private int maxNumTries = 3;
    private boolean isInitialized = false;
}
