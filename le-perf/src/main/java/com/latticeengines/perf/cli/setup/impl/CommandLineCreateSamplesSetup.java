package com.latticeengines.perf.cli.setup.impl;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingElement;
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
        Option customer = new CommandLineOption(CUSTOMER_OPT, CUSTOMER_LONGOPT, true, true, CUSTOMER_DEF);
        Option table = new CommandLineOption(TABLE_OPT, TABLE_LONGOPT, true, true, TABLE_DEF);
        Option trainingPercentage = new CommandLineOption(TRAINING_PERCENTAGE_OPT, TRAINING_PERCENTAGE_LONGOPT, true,
                true, TRAINING_PERCENTAGE_DEF);
        Option numOfSamples = new CommandLineOption(NUMOFSAMPLES_OPT, NUMOFSAMPLES_LONGOPT, true, true,
                NUMOFSAMPLES_DEF);
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

        String table = cl.getOptionValue(TABLE_OPT);
        String trainingPercentage = cl.getOptionValue(TRAINING_PERCENTAGE_OPT);
        String numOfSamples = cl.getOptionValue(NUMOFSAMPLES_OPT);

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
