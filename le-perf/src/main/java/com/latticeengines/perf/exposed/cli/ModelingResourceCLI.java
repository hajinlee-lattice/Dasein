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
import org.apache.commons.cli.ParseException;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.perf.cli.setup.CommandLineSetup;
import com.latticeengines.perf.cli.setup.factory.CommandLineSetupFactory;
import com.latticeengines.perf.job.properties.CommandLineProperties;
import com.latticeengines.perf.job.runnable.ModelingResourceJob;
import com.latticeengines.perf.job.runnable.impl.GetJobStatus;

public class ModelingResourceCLI {

    private static CommandLine cl;
    private static ExecutorService executor;
    private static List<Future<List<String>>> futures = new ArrayList<Future<List<String>>>();

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

    private static void executeJob(CommandLineSetup cmdlstp) throws Exception {
        int numOfCustomers = Integer.parseInt(cl.getOptionValue(CommandLineProperties.CUSTOMER_OPT));
        int customerID = 0;
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
        System.out.println(futures.size());

        for (Future<List<String>> future : futures) {
            applicationIds.addAll(future.get());
        }

        executor.shutdown();
        Thread.sleep(30000L);

        for (int i = 0; i < 8; i++) {
            for (String appId : applicationIds) {
                GetJobStatus gjs = new GetJobStatus();
                gjs.setConfiguration("localhost:8080", appId);
                JobStatus js = gjs.getJobStatus();
                System.out.println(js.getStatus());
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
        int numOfThreads = Integer.parseInt(command[3]);
        executor = Executors.newFixedThreadPool(numOfThreads);

        CommandLineSetup cmdlstp = CommandLineSetupFactory.create(command);
        cl = cmdlstp.getCommandLine();
        // Class<? extends ModelingResourceJob> cls = (Class<? extends
        // ModelingResourceJob>) cmdlstp.getClass();
        executeJob(cmdlstp);
    }

}
