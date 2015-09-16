package com.latticeengines.propdata.eai.entitymanager;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.CommandIds;
import com.latticeengines.domain.exposed.propdata.Commands;
import com.latticeengines.propdata.eai.entitymanager.PropDataEntityMgr;

@ContextConfiguration(locations = { "classpath:propdata-db-context.xml", "classpath:propdata-properties-context.xml" })
public class PropDataEntityMgrImplTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private PropDataEntityMgr propDataEntityMgr;

    @Test(groups = "functional", enabled=false)
    public void createCommands() throws Exception {
        Commands commands = PropDataTestUtils.createNewCommands("Docusign", "Match_source_ForPropDataMatching999");
        propDataEntityMgr.createCommands(commands);

        Assert.assertNotNull(commands.getPid());
        CommandIds commandIds = commands.getCommandIds();
        Assert.assertEquals(commands.getPid(), commandIds.getPid());

        Commands actualCommands = propDataEntityMgr.getCommands(commands.getPid());
        CommandIds actualCommandIds = actualCommands.getCommandIds();

        assertCommands(commands, actualCommands);
        assertCommandIds(commandIds, actualCommandIds);
    }

    private void assertCommandIds(CommandIds commandIds, CommandIds actualCommandIds) {
        Assert.assertEquals(actualCommandIds.getPid(), commandIds.getPid());
        Assert.assertEquals(actualCommandIds.getCreatedBy(), commandIds.getCreatedBy());
        // Assert.assertEquals(actualCommandIds.getCreateTime(),
        // commandIds.getCreateTime());
    }

    private void assertCommands(Commands commands, Commands actualCommands) {
        Assert.assertEquals(actualCommands.getPid(), commands.getPid());
        Assert.assertEquals(actualCommands.getCommandName(), commands.getCommandName());
        Assert.assertEquals(actualCommands.getContractExternalID(), commands.getContractExternalID());
        Assert.assertEquals(actualCommands.getDeploymentExternalID(), commands.getDeploymentExternalID());
        Assert.assertEquals(actualCommands.getDestTables(), commands.getDestTables());
        Assert.assertEquals(actualCommands.getProcessUID(), commands.getProcessUID());
        Assert.assertEquals(actualCommands.getProfileID(), commands.getProfileID());
        Assert.assertEquals(actualCommands.getSourceTable(), commands.getSourceTable());
        // Assert.assertEquals(actualCommands.getCommandStatus(),
        // commands.getCommandStatus());
        // Assert.assertEquals(actualCommands.getCreateTime(),
        // commands.getCreateTime());
        Assert.assertEquals(actualCommands.getIsDownloading(), commands.getIsDownloading());
        Assert.assertEquals(actualCommands.getMaxNumRetries(), commands.getMaxNumRetries());
        Assert.assertEquals(actualCommands.getNumRetries(), commands.getNumRetries());

    }

}
