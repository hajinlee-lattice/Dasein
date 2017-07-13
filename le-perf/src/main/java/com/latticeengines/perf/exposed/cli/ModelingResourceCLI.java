package com.latticeengines.perf.exposed.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.perf.cli.setup.CommandLineSetup;
import com.latticeengines.perf.cli.setup.factory.CommandLineSetupFactory;
import com.latticeengines.perf.job.properties.CommandLineProperties;
import com.latticeengines.perf.job.runnable.ModelingResourceJob;
import com.latticeengines.perf.job.runnable.impl.GetJobStatus;

public class ModelingResourceCLI {

    private static CommandLine cl;
    private static ExecutorService executor;
    private static final Logger log = LoggerFactory.getLogger(ModelingResourceCLI.class);

    public static void main(String[] args) throws Exception {
        if (args.length > 0)
            executeJob(args);
        else {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
                String line = "";
                try {
                    while ((line = br.readLine()) != null) {
                        String[] command = line.split(" ");
                        executeJob(command);
                    }
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static void executeJob(CommandLineSetup cmdlstp, String restEndpointHost) throws Exception {
        int numOfCustomers = Integer.parseInt(cl.getOptionValue(CommandLineProperties.CUSTOMER_OPT));
        int customerID = 0;
        List<Future<List<String>>> futures = new ArrayList<Future<List<String>>>();
        while (customerID < numOfCustomers) {
            Class<? extends ModelingResourceJob> cls = cmdlstp.getJobClassType();
            Constructor<?> ctor = cls.getConstructor();
            ModelingResourceJob worker = (ModelingResourceJob) ctor.newInstance();
            cmdlstp.setCustomer("c" + customerID);
            cmdlstp.setConfiguration(worker);
            Future<List<String>> appIdList = executor.submit(worker);
            futures.add(appIdList);
            customerID++;
        }

        List<String> applicationIds = new ArrayList<String>();

        for (Future<List<String>> future : futures) {
            applicationIds.addAll(future.get());
        }

        executor.shutdown();
        Thread.sleep(30000L);
        GetJobStatus.checkStatus(restEndpointHost, applicationIds);
        System.out.println(applicationIds);
    }

    private static void executeJob(String[] command) throws Exception {
        if (command.length < 5) {
            throw new Exception("Too few arguments for the legal command");
        }
        if (!command[0].equalsIgnoreCase("ledp")) {
            throw new Exception("Unrecognized command type. Please start your command with 'ledp'");
        }
        int numOfThreads = Integer.parseInt(command[3]);
        executor = Executors.newFixedThreadPool(numOfThreads);

        @SuppressWarnings("rawtypes")
        CommandLineSetup cmdlstp = CommandLineSetupFactory.create(command);
        cl = cmdlstp.getCommandLine();
        // Class<? extends ModelingResourceJob> cls = (Class<? extends
        // ModelingResourceJob>) cmdlstp.getClass();
        executeJob(cmdlstp, command[2]);
    }
}
