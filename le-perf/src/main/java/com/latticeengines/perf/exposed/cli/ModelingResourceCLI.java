package com.latticeengines.perf.exposed.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.perf.cli.setup.CommandLineSetup;
import com.latticeengines.perf.job.clisetup.factory.ModelingResourceJobFactory;
import com.latticeengines.perf.job.runnable.ModelingResourceJob;
import com.latticeengines.perf.job.runnable.impl.GetJobStatus;

public class ModelingResourceCLI {

    private static RestTemplate restTemplate = new RestTemplate();
    private static CommandLine cl;
    private static ExecutorService executor;
    private static String restEndpointHost;
    private static ConcurrentLinkedQueue<Future<List<String>>> appIdQueue = new ConcurrentLinkedQueue<Future<List<String>>>();

    public static void main(String[] args) throws IOException, ParseException, Exception {
        if (args.length > 0)
            executeJob(args);
        else {
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            String line = null;
            try {
                while ((line = br.readLine()) != null) {
                    String[] command = line.split(" ");
                    executeJob(command);
                }
            } finally {
                try {
                    br.close();
                } catch (IOException e) {
                    throw e;
                }
            }
        }
    }

    private static void executeJob(Class<? extends ModelingResourceJob> cls) throws Exception {
        int numOfCustomers = Integer.parseInt(cl.getOptionValue("c"));
        int customerID = 0;
        while (customerID < numOfCustomers) {
            Constructor<?> ctor = cls.getConstructor(CommandLine.class, String.class, String.class);
            ModelingResourceJob worker = (ModelingResourceJob) ctor.newInstance(cl, "c" + customerID, restEndpointHost);
            Future<List<String>> appIdList = executor.submit(worker);
            appIdQueue.offer(appIdList);
            customerID++;
        }

        List<String> applicationIds = new ArrayList<String>();
        System.out.println(appIdQueue.size());
        while (appIdQueue.size() == numOfCustomers) {
            for (Future<List<String>> future : appIdQueue) {
                applicationIds.addAll(future.get());
            }
            appIdQueue.clear();
        }
        executor.shutdown();
        Thread.sleep(30000L);

        for (int i = 0; i < 8; i++) {
            for (String appId : applicationIds) {
                GetJobStatus gjs = new GetJobStatus();
                gjs.setLedpRestClient(restEndpointHost);
                JobStatus js = gjs.getJobStatus(appId);
                System.out.println(js.getState());
            }
        }
        System.out.println(applicationIds);
    }

    private static void executeJob(String[] command) throws Exception {
        if (command.length < 5) {
            throw new Exception("Too few arguments for the legal command");
        }
        if (!command[0].equalsIgnoreCase("ledp")) {
            throw new Exception("Unrecognized command type. Please start your command with 'ledp'");
        }
        int numOfThreads = Integer.parseInt(command[2]);
        executor = Executors.newFixedThreadPool(numOfThreads);
        restEndpointHost = command[3];

        CommandLineSetup cmdlstp = ModelingResourceJobFactory.create(command);
        cl = cmdlstp.getCommandLine();
        Class<? extends ModelingResourceJob> cls = (Class<? extends ModelingResourceJob>) cmdlstp.getClass();
        executeJob(cls);
    }

}
