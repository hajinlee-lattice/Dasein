package com.latticeengines.perf.job.runnable.commandline.impl;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import com.latticeengines.domain.exposed.dataplatform.DataProfileConfiguration;
import com.latticeengines.perf.cli.setup.CommandLineOption;
import com.latticeengines.perf.cli.setup.CommandLineSetup;
import com.latticeengines.perf.job.runnable.impl.Profile;

public class CommandLineProfile extends Profile implements CommandLineSetup {

    private CommandLine cl;

    public CommandLineProfile() {
    }

    public CommandLineProfile(CommandLine cl, String customer, String restEndpointHost) {
        super(customer, restEndpointHost);
        this.cl = cl;
    }

    @Override
    public DataProfileConfiguration setConfiguration() {
        DataProfileConfiguration config = new DataProfileConfiguration();
        config.setCustomer(customer);
        config.setTable(cl.getOptionValue("t"));
        config.setMetadataTable(cl.getOptionValue("mt"));
        config.setSamplePrefix("all");
        config.setExcludeColumnList(createExcludeList());
        return config;
    }

    private List<String> createExcludeList() {
        List<String> excludeList = new ArrayList<>();
        excludeList.add("Nutanix_EventTable_Clean");
        excludeList.add("P1_Event");
        excludeList.add("P1_Target");
        excludeList.add("P1_TargetTraining");
        excludeList.add("PeriodID");
        excludeList.add("CustomerID");
        excludeList.add("AwardYear");
        excludeList.add("FundingFiscalQuarter");
        excludeList.add("FundingFiscalYear");
        excludeList.add("BusinessAssets");
        excludeList.add("BusinessEntityType");
        excludeList.add("BusinessIndustrySector");
        excludeList.add("RetirementAssetsYOY");
        excludeList.add("RetirementAssetsEOY");
        excludeList.add("TotalParticipantsSOY");
        excludeList.add("BusinessType");
        excludeList.add("LeadID");
        excludeList.add("Company");
        excludeList.add("Domain");
        excludeList.add("Email");
        excludeList.add("LeadSource");
        return excludeList;
    }

    public void setupOptions(String[] args) throws ParseException {
        Options ops = new Options();
        Option customer = new CommandLineOption("c", "customer", true, true, "Number OF Customers sending requests");
        Option table = new CommandLineOption("t", "table", true, true, "Table Name");
        Option metadataTable = new CommandLineOption("mt", "metadatatable", true, true, "Metadata Table Name");
        Option keyCol = new CommandLineOption("kc", "keycolumn", true, true, "Key Column Name");
        ops.addOption(customer).addOption(table).addOption(metadataTable).addOption(keyCol);
        cl = clp.parse(ops, args);
    }

    @Override
    public CommandLine getCommandLine() {
        return cl;
    }
}
