package com.latticeengines.propdata.eai.entitymanager;

import java.util.Date;

import com.latticeengines.domain.exposed.propdata.CommandIds;
import com.latticeengines.domain.exposed.propdata.Commands;

public class PropDataTestUtils {

    public static Commands createNewCommands(String customer, String table) {

        CommandIds commandIds = new CommandIds();
        commandIds.setCreatedBy("propdata@lattice-engines.com");
        Date now = new Date();
        commandIds.setCreateTime(now);

        Commands commands = new Commands();
        commands.setCommandIds(commandIds);
        commands.setCommandName("RunMatchWithLEUniverse");
        commands.setCommandStatus(0);
        commands.setContractExternalID(customer);
        commands.setCreateTime(now);
        commands.setDeploymentExternalID(customer);
        commands.setDestTables("DerivedColumns|Experian_Source");
        commands.setIsDownloading(false);
        commands.setMaxNumRetries(5);
        commands.setNumRetries(0);
        commands.setProcessUID("624AD64A-7B8F-489B-87F5-B685B7E78383");
        commands.setSourceTable(table);
        return commands;
    }
}