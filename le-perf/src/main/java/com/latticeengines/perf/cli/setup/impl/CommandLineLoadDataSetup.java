package com.latticeengines.perf.cli.setup.impl;

import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.perf.cli.setup.CommandLineOption;
import com.latticeengines.perf.cli.setup.CommandLineSetup;
import com.latticeengines.perf.job.runnable.impl.LoadData;

public class CommandLineLoadDataSetup extends CommandLineSetup<LoadData> {

    private CommandLine cl;

    public CommandLineLoadDataSetup(String restEndpointHost) {
        super(restEndpointHost);
    }

    public void setupOptions(String[] args) throws ParseException {
        Options ops = new Options();
        Option host = new CommandLineOption(HOST_OPT, HOST_LONGOPT, true, true, HOST_DEF);
        Option port = new CommandLineOption(PORT_OPT, PORT_LONGOPT, true, true, PORT_DEF);
        Option dbtp = new CommandLineOption(DATABASE_TYPE_OPT, DATABASE_TYPE_LONGOPT, true, true, DATABASE_TYPE_DEF);
        Option db = new CommandLineOption(DATABASE_OPT, DATABASE_LONGOPT, true, true, DATABASE_DEF);
        Option user = new CommandLineOption(USER_OPT, USER_LONGOPT, true, true, USER_DEF);
        Option passwd = new CommandLineOption(PASSWORD_OPT, PASSWORD_LONGOPT, true, true, PASSWORD_DEF);
        Option customer = new CommandLineOption(CUSTOMER_OPT, CUSTOMER_LONGOPT, true, true, CUSTOMER_DEF);
        Option table = new CommandLineOption(TABLE_OPT, TABLE_LONGOPT, true, true, TABLE_DEF);
        Option keyCol = new CommandLineOption(KEYCOLUMN_OPT, KEYCOLUMN_LONGOPT, true, true, KEYCOLUMN_DEF);
        Option metadataTable = new CommandLineOption(METADATA_TABLE_OPT, METADATA_TABLE_LONGOPT, true, false,
                METADATA_TABLE_DEF);

        ops.addOption(host).addOption(port).addOption(dbtp).addOption(db).addOption(user).addOption(passwd)//
                .addOption(customer).addOption(table).addOption(keyCol)//
                .addOption(metadataTable);
        this.cl = clp.parse(ops, args);
    }

    @Override
    public CommandLine getCommandLine() {
        return cl;
    }

    @Override
    public Class<LoadData> getJobClassType() {
        return LoadData.class;
    }

    public void setConfiguration(LoadData ld) throws Exception {
        LoadConfiguration config = new LoadConfiguration();
        DbCreds.Builder builder = new DbCreds.Builder();

        String host = cl.getOptionValue(HOST_OPT);
        String port = cl.getOptionValue(PORT_OPT);
        String dbtp = cl.getOptionValue(DATABASE_TYPE_OPT);
        String db = cl.getOptionValue(DATABASE_OPT);
        String user = cl.getOptionValue(USER_OPT);
        String passwd = cl.getOptionValue(PASSWORD_OPT);
        String table = cl.getOptionValue(TABLE_OPT);
        String keyCol = cl.getOptionValue(KEYCOLUMN_OPT);
        String metadataTable = cl.getOptionValue(METADATA_TABLE_OPT);

        builder.host(host).port(Integer.parseInt(port)).db(db).user(user).clearTextPassword(passwd).dbType(dbtp);
        DbCreds dc = new DbCreds(builder);
        config.setCustomer(customer);
        config.setTable(table);
        config.setMetadataTable(metadataTable);
        config.setKeyCols(Arrays.<String> asList(keyCol.split(VALUE_DELIMETER)));
        config.setCreds(dc);
        ld.setConfiguration(restEndpointHost, config);
    }
}
