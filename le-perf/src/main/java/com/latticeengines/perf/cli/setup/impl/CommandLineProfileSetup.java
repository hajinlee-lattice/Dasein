package com.latticeengines.perf.cli.setup.impl;

import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.perf.cli.setup.CommandLineOption;
import com.latticeengines.perf.cli.setup.CommandLineSetup;
import com.latticeengines.perf.job.properties.CommandLineProperties;
import com.latticeengines.perf.job.runnable.impl.Profile;

public class CommandLineProfileSetup extends CommandLineSetup<Profile> {

    private CommandLine cl;

    public CommandLineProfileSetup(String restEndpointHost) {
        super(restEndpointHost);
    }

    public void setupOptions(String[] args) throws ParseException {
        Options ops = new Options();
        Option customer = new CommandLineOption(CUSTOMER_OPT, CUSTOMER_LONGOPT, true, true, CUSTOMER_DEF);
        Option table = new CommandLineOption(TABLE_OPT, TABLE_LONGOPT, true, true, TABLE_DEF);
        Option target = new CommandLineOption(TARGET_OPT, TARGET_LONGOPT, true, false, TARGET_DEF);
        Option metadataTable = new CommandLineOption(METADATA_TABLE_OPT, METADATA_TABLE_LONGOPT, true, true,
                METADATA_TABLE_DEF);
        Option algorithmProps = new CommandLineOption(ALGORITHM_PROP_OPT, ALGORITHM_PROP_LONGOPT, true, false,
                ALGORITHM_PROP_DEF);
        ops.addOption(customer).addOption(table).addOption(target).addOption(metadataTable).addOption(algorithmProps);
        cl = clp.parse(ops, args);
    }

    @Override
    public CommandLine getCommandLine() {
        return cl;
    }

    @Override
    public Class<Profile> getJobClassType() {
        return Profile.class;
    }

    public void setConfiguration(Profile pf) throws Exception {
        DataProfileConfiguration config = new DataProfileConfiguration();
        config.setCustomer(customer);
        config.setTable(cl.getOptionValue(TABLE_OPT));
        config.setMetadataTable(cl.getOptionValue(METADATA_TABLE_OPT));
        config.setSamplePrefix("all");
        config.setExcludeColumnList(Profile.createExcludeList());
        String targets = cl.getOptionValue(TARGET_OPT);
        if (targets == null || targets.length() == 0) {
            config.setTargets(Arrays.<String> asList(new String[] { "P1_Event" }));
        } else {
            config.setTargets(Arrays.<String> asList(targets.split(CommandLineProperties.VALUE_DELIMETER)));
        }
        pf.setConfiguration(restEndpointHost, config);

        String algorithmProps = cl.getOptionValue(ALGORITHM_PROP_OPT);
        if (StringUtils.isNotEmpty(algorithmProps)) {
            String[] propList = algorithmProps.split(CommandLineProperties.VALUE_DELIMETER);
            String name = propList.length > 0 ? propList[0] : "";
            String virtualCores = propList.length > 1 ? propList[1] : "";
            String memory = propList.length > 2 ? propList[2] : "";
            String priority = propList.length > 3 ? propList[3] : "";
            if (name.equalsIgnoreCase(ALGORITHM_NAME_PROFILE)) {
                configAlgorithm(config, virtualCores, memory, priority);
            }
        }
    }

    static void configAlgorithm(DataProfileConfiguration cponfig, String virtualCores, String memory, String priority) {
        if (StringUtils.isEmpty(virtualCores) || StringUtils.isEmpty(memory) || StringUtils.isEmpty(priority)) {
            return;
        }
        cponfig.setContainerProperties(new StringBuilder().append("VIRTUALCORES=")//
                .append(virtualCores).append(" MEMORY=").append(memory)//
                .append(" PRIORITY=").append(priority).toString());
    }
}
