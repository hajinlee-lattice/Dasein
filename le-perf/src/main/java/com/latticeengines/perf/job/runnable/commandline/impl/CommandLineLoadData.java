package com.latticeengines.perf.job.runnable.commandline.impl;

import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.latticeengines.domain.exposed.dataplatform.DbCreds;
import com.latticeengines.domain.exposed.dataplatform.LoadConfiguration;
import com.latticeengines.perf.cli.setup.CommandLineOption;
import com.latticeengines.perf.cli.setup.CommandLineSetup;
import com.latticeengines.perf.job.properties.CommandLineProperties;
import com.latticeengines.perf.job.runnable.impl.LoadData;

public class CommandLineLoadData extends LoadData implements CommandLineSetup {

    private CommandLine cl;

    public CommandLineLoadData() {
    }

    public CommandLineLoadData(CommandLine cl, String customer, String restEndpointHost) {
        super(customer, restEndpointHost);
        this.cl = cl;
    }

    public LoadConfiguration setConfiguration() {
        LoadConfiguration config = new LoadConfiguration();
        DbCreds.Builder builder = new DbCreds.Builder();
        String host = cl.getOptionValue("H");
        String port = cl.getOptionValue("P");
        String dbtp = cl.getOptionValue("dbtp");
        String db = cl.getOptionValue("db");
        String user = cl.getOptionValue("u");
        String passwd = cl.getOptionValue("ps");
        String table = cl.getOptionValue("t");
        String keyCol = cl.getOptionValue("kc");
        String metadataTable = cl.getOptionValue("mt");

        builder.host(host).port(Integer.parseInt(port)).db(db).user(user).password(passwd).type(dbtp);
        DbCreds dc = new DbCreds(builder);
        config.setCustomer(customer);
        config.setTable(table);
        config.setMetadataTable(metadataTable);
        config.setKeyCols(Arrays.<String> asList(keyCol.split(CommandLineProperties.DELIMETER)));
        config.setCreds(dc);

        return config;
    }

    public void setupOptions(String[] args) throws ParseException {
        Options ops = new Options();
        Option host = new CommandLineOption("H", "host", true, true, "Datasource Host Address");
        Option port = new CommandLineOption("P", "port", true, true, "Datasource port");
        Option dbtp = new CommandLineOption("dbtp", "databasetype", true, true, "Datasource type");
        Option db = new CommandLineOption("db", "database", true, true, "Datasource Database Name");
        Option user = new CommandLineOption("u", "user", true, true, "Datasource User Name");
        Option passwd = new CommandLineOption("ps", "passowrd", true, true, "Datasource password");
        Option customer = new CommandLineOption("c", "customer", true, true, "Number OF Customers sending requests");
        Option table = new CommandLineOption("t", "table", true, true, "Table Name");
        Option keyCol = new CommandLineOption("kc", "keycolumn", true, true, "Key Column Name");
        Option metadataTable = new CommandLineOption("mt", "metadatatable", true, false, "Metadata Table Name");

        ops.addOption(host).addOption(port).addOption(dbtp).addOption(db).addOption(user).addOption(passwd)//
                .addOption(customer).addOption(table).addOption(keyCol)//
                .addOption(metadataTable);
        this.cl = clp.parse(ops, args);
    }

    @Override
    public CommandLine getCommandLine() {
        return cl;
    }
}
