package com.latticeengines.perf.cli.setup.impl;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import com.latticeengines.domain.exposed.dataplatform.DataProfileConfiguration;
import com.latticeengines.perf.cli.setup.CommandLineOption;
import com.latticeengines.perf.cli.setup.CommandLineSetup;
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
        Option metadataTable = new CommandLineOption(METADATA_TABLE_OPT, METADATA_TABLE_LONGOPT, true, true,
                METADATA_TABLE_DEF);
        ops.addOption(customer).addOption(table).addOption(metadataTable);
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

        pf.setConfiguration(restEndpointHost, config);
    }
}
