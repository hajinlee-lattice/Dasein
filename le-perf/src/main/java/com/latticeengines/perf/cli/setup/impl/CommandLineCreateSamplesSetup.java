package com.latticeengines.perf.cli.setup.impl;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.latticeengines.domain.exposed.dataplatform.SamplingConfiguration;
import com.latticeengines.domain.exposed.dataplatform.SamplingElement;
import com.latticeengines.perf.cli.setup.CommandLineOption;
import com.latticeengines.perf.cli.setup.CommandLineSetup;
import com.latticeengines.perf.job.runnable.impl.CreateSamples;

public class CommandLineCreateSamplesSetup extends CommandLineSetup<CreateSamples> {

    private CommandLine cl;

    public CommandLineCreateSamplesSetup(String restEndpointHost) {
        super(restEndpointHost);
    }

    public void setupOptions(String[] args) throws ParseException {
        Options ops = new Options();
        Option customer = new CommandLineOption("c", "customer", true, true, "Number OF Customers sending requests");
        Option table = new CommandLineOption("t", "table", true, true, "Table Name");
        Option trainingPercentage = new CommandLineOption("tp", "trainingpercentage", true, true, "Training Percentage");
        Option numOfSamples = new CommandLineOption("N", "samplesnumber", true, true, "Number of Samples to create");
        ops.addOption(customer).addOption(table).addOption(trainingPercentage).addOption(numOfSamples);
        cl = clp.parse(ops, args);
    }

    @Override
    public CommandLine getCommandLine() {
        return cl;
    }

    public Class<CreateSamples> getJobClassType() {
        return CreateSamples.class;
    }

    public void setConfiguration(CreateSamples cs) throws Exception {
        SamplingConfiguration config = new SamplingConfiguration();

        String table = cl.getOptionValue("t");
        String trainingPercentage = cl.getOptionValue("tp");
        String numOfSamples = cl.getOptionValue("N");

        config.setCustomer(customer);
        config.setTable(table);
        config.setTrainingPercentage(Integer.parseInt(trainingPercentage));
        int num = Integer.parseInt(numOfSamples), value = 100 / num;
        SamplingElement se = null;
        for (int i = 0; i < num - 1; i++) {
            String name = "s" + i;
            se = new SamplingElement();
            se.setName(name);
            se.setPercentage(value);
            config.addSamplingElement(se);
            value *= i + 2;
        }
        se = new SamplingElement();
        se.setName("all");
        se.setPercentage(100);
        config.addSamplingElement(se);

        cs.setConfiguration(restEndpointHost, config);
    }
}
