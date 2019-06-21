/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flink.cascading.planner;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.client.program.OptimizerPlanEnvironment;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataartisans.flink.cascading.runtime.stats.AccumulatorCache;
import com.dataartisans.flink.cascading.util.FlinkConfigConstants;

import cascading.flow.planner.FlowStepJob;
import cascading.management.state.ClientState;
import cascading.stats.FlowNodeStats;
import cascading.stats.FlowStepStats;
import scala.concurrent.duration.FiniteDuration;

public class FlinkFlowStepJob extends FlowStepJob<Configuration> {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkFlowStepJob.class);
    private static final int accumulatorUpdateIntervalSecs = 10;
    private static final Object lock = new Object();
    @SuppressWarnings("unused")
    private static final FiniteDuration DEFAULT_TIMEOUT = new FiniteDuration(60, TimeUnit.SECONDS);
    private volatile static MiniCluster localCluster;
    private volatile static int localClusterUsers;
    private final Configuration currentConf;
    private final ExecutionEnvironment env;
    private ClusterClient client;
    private JobID jobID;
    private Throwable jobException;
    private List<String> classPath;
    private AccumulatorCache accumulatorCache;
    private Future<JobSubmissionResult> jobSubmission;
    private ExecutorService executorService = Executors.newFixedThreadPool(1);

    public FlinkFlowStepJob(ClientState clientState, FlinkFlowStep flowStep,
            Configuration currentConf, List<String> classPath) {

        super(clientState, currentConf, flowStep, 1000, 60000, 60000);

        this.currentConf = currentConf;
        this.env = ((FlinkFlowStep) this.flowStep).getExecutionEnvironment();
        this.classPath = classPath;

        if (flowStep.isDebugEnabled()) {
            flowStep.logDebug("using polling interval: " + pollingInterval);
        }
    }

    @Override
    public Configuration getConfig() {
        return currentConf;
    }

    @Override
    protected FlowStepStats createStepStats(ClientState clientState) {
        this.accumulatorCache = new AccumulatorCache(accumulatorUpdateIntervalSecs);
        return new FlinkFlowStepStats(this.flowStep, clientState, accumulatorCache);
    }

    protected void internalBlockOnStop() throws IOException {

        if (jobSubmission != null && !jobSubmission.isDone()) {
            try {
                client.cancel(jobID);
            } catch (Exception e) {
                throw new IOException("An exception occurred while stopping the Flink job with ID: "
                        + jobID + ": " + e.getMessage());
            }
        }

    }

    protected void internalNonBlockingStart() throws IOException {

        Plan plan = env.createProgramPlan();

        // set exchange mode, BATCH is default
        String execMode = getConfig().get(FlinkConfigConstants.EXECUTION_MODE);
        if (execMode == null || FlinkConfigConstants.EXECUTION_MODE_BATCH.equals(execMode)) {
            env.getConfig().setExecutionMode(ExecutionMode.BATCH);
        } else if (FlinkConfigConstants.EXECUTION_MODE_PIPELINED.equals(execMode)) {
            env.getConfig().setExecutionMode(ExecutionMode.PIPELINED);
        } else {
            LOG.warn("Unknow value for '" + FlinkConfigConstants.EXECUTION_MODE + "' parameter. "
                    + "Only '" + FlinkConfigConstants.EXECUTION_MODE_BATCH + "' " + "or '"
                    + FlinkConfigConstants.EXECUTION_MODE_PIPELINED + "' supported. " + "Using "
                    + FlinkConfigConstants.EXECUTION_MODE_BATCH + " exchange by default.");
            env.getConfig().setExecutionMode(ExecutionMode.BATCH);
        }

        Optimizer optimizer = new Optimizer(new DataStatistics(),
                new org.apache.flink.configuration.Configuration());
        OptimizedPlan optimizedPlan = optimizer.compile(plan);

        final JobGraph jobGraph = new JobGraphGenerator().compileJobGraph(optimizedPlan);
        for (String jarPath : classPath) {
            jobGraph.addJar(new Path(jarPath));
        }

        jobID = jobGraph.getJobID();
        accumulatorCache.setJobID(jobID);

        if (isLocalExecution()) {

            flowStep.logInfo("Executing in local mode.");

            startLocalCluster();

            org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();

            final String clusterHostName = localCluster.getClusterInformation().getBlobServerHostname();
            final int jobManagerPort = localCluster.getClusterInformation().getBlobServerPort();
            config.setString(JobManagerOptions.ADDRESS, clusterHostName);
            config.setInteger(JobManagerOptions.PORT, jobManagerPort);
//            config.setString(AkkaOptions.ASK_TIMEOUT, "600s");
//            config.setString(AkkaOptions.FRAMESIZE, "1g");
            flowStep.logWarn("Using local cluster at " + clusterHostName + " JM port: " + jobManagerPort);

            client = new MiniClusterClient(config, localCluster);
            client.setPrintStatusDuringExecution(env.getConfig().isSysoutLoggingEnabled());

        } else {

            flowStep.logInfo("Executing in cluster mode.");

            try {
                String path = this.getClass().getProtectionDomain().getCodeSource().getLocation()
                        .toURI().getPath();
                jobGraph.addJar(new Path(path));
                classPath.add(path);
            } catch (URISyntaxException e) {
                throw new IOException("Could not add the submission JAR as a dependency.");
            }

            client = ((ContextEnvironment) env).getClient();
        }

        List<URL> fileList = new ArrayList<>(classPath.size());
        for (String path : classPath) {
            URL url;
            try {
                url = new URL(path);
            } catch (MalformedURLException e) {
                url = new URL("file://" + path);
            }
            fileList.add(url);
        }

        final ClassLoader loader = JobWithJars.buildUserCodeClassLoader(fileList,
                Collections.emptyList(), getClass().getClassLoader());

        accumulatorCache.setClient(client);

        final Callable<JobSubmissionResult> callable = () -> client.submitJob(jobGraph, loader);

        jobSubmission = executorService.submit(callable);

        flowStep.logInfo("submitted Flink job: " + jobID);
    }

    @Override
    protected void updateNodeStatus(FlowNodeStats flowNodeStats) {
        try {
            if (internalNonBlockingIsComplete() && internalNonBlockingIsSuccessful()) {
                flowNodeStats.markSuccessful();
            } else if (internalIsStartedRunning()) {
                flowNodeStats.isRunning();
            } else {
                flowNodeStats.markFailed(jobException);
            }
        } catch (IOException e) {
            flowStep.logError("Failed to update node status.");
        }
    }

    protected boolean internalNonBlockingIsSuccessful() throws IOException {
        try {
            jobSubmission.get(0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | TimeoutException e) {
            return false;
        } catch (ExecutionException e) {
            jobException = e.getCause();
            return false;
        }

        boolean isDone = jobSubmission.isDone();
        if (isDone) {
            accumulatorCache.update(true);
            accumulatorCache.setJobID(null);
            accumulatorCache.setClient(null);
            stopCluster();
        }

        return isDone;
    }

    @Override
    public Throwable call() {
        if (env instanceof OptimizerPlanEnvironment) {
            // We have an OptimizerPlanEnvironment.
            // This environment is only used to to fetch the Flink execution
            // plan.
            try {
                // OptimizerPlanEnvironment does not execute but only build the
                // execution plan.
                env.execute("plan generation");
            }
            // execute() throws a ProgramAbortException if everything goes well
            catch (OptimizerPlanEnvironment.ProgramAbortException pae) {
                // Forward call() to get Cascading's internal job stats right.
                // The job will be skipped due to the overridden isSkipFlowStep
                // method.
                super.call();
                // forward expected ProgramAbortException
                return pae;
            }
            //
            catch (Exception e) {
                // forward unexpected exception
                return e;
            }
        }
        // forward to call() if we have a regular ExecutionEnvironment
        return super.call();

    }

    protected boolean isSkipFlowStep() throws IOException {
        if (env instanceof OptimizerPlanEnvironment) {
            // We have an OptimizerPlanEnvironment.
            // This environment is only used to to fetch the Flink execution
            // plan.
            // We do not want to execute the job in this case.
            return true;
        } else {
            return super.isSkipFlowStep();
        }
    }

    @Override
    protected boolean isRemoteExecution() {
        return env instanceof ContextEnvironment;
    }

    @Override
    protected Throwable getThrowable() {
        return jobException;
    }

    protected String internalJobId() {
        return jobID.toString();
    }

    protected boolean internalNonBlockingIsComplete() {
        return jobSubmission.isDone();
    }

    protected void dumpDebugInfo() {
    }

    protected boolean internalIsStartedRunning() {
        return jobSubmission != null;
    }

    private boolean isLocalExecution() {
        return env instanceof LocalEnvironment;
    }

    private void startLocalCluster() {
        synchronized (lock) {
            if (localCluster == null) {
                org.apache.flink.configuration.Configuration configuration = new org.apache.flink.configuration.Configuration();
                configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, env.getParallelism() * 2);
                configuration.setString(AkkaOptions.ASK_TIMEOUT, "600s");
                configuration.setString(AkkaOptions.FRAMESIZE, "1g");
                LOG.info("Creating a new Flink MiniCluster.");
                configuration.setString(RestOptions.ADDRESS, "localhost");
                MiniClusterConfiguration miniConf =
                        new MiniClusterConfiguration(configuration, 1, RpcServiceSharing.DEDICATED, null);
                localCluster = new MiniCluster(miniConf);
                try {
                    localCluster.start();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to start flink MiniCluster.", e);
                }
            }
            localClusterUsers++;
        }
    }

    private void stopCluster() {
        synchronized (lock) {
            if (localCluster != null) {
                if (--localClusterUsers <= 0) {
                    localCluster.closeAsync().join();
                    localCluster = null;
                    localClusterUsers = 0;
                }
            }
            if (executorService != null) {
                executorService.shutdown();
            }
        }
    }

}
