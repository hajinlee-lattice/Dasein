package com.lattice.workflow.action;

import java.util.Map;
import java.util.Properties;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.propdata.service.db.PropDataContext;
import com.latticeengines.propdata.service.db.PropDataDBService;
import com.latticeengines.propdata.service.db.PropDataKey.CommandIdsKey;
import com.latticeengines.propdata.service.db.PropDataKey.CommandsKey;
import com.latticeengines.propdata.service.db.PropDataKey.ImportExportKey;

public class PropDataAddCommandAction extends AbstractAction<PropDataContext, PropDataContext> {

    @Autowired
    private PropDataDBService propDataDBService;

    @Override
    PropDataContext getRequest(Map<String, String> argMap) {
        PropDataContext request = new PropDataContext();

        request.setProperty(CommandIdsKey.CREATED_BY.getKey(),
                getMappedValue(argMap.get(CommandIdsKey.CREATED_BY.getKey()), String.class));

        request.setProperty(CommandsKey.COMMAND_NAME.getKey(),
                getMappedValue(argMap.get(CommandsKey.COMMAND_NAME.getKey()), String.class));
        request.setProperty(CommandsKey.CONTRACT_EXTERNAL_ID.getKey(),
                getMappedValue(argMap.get(CommandsKey.CONTRACT_EXTERNAL_ID.getKey()), String.class));
        request.setProperty(CommandsKey.DEPLOYMENT_EXTERNAL_ID.getKey(),
                getMappedValue(argMap.get(CommandsKey.DEPLOYMENT_EXTERNAL_ID.getKey()), String.class));
        request.setProperty(CommandsKey.DESTTABLES.getKey(),
                getMappedValue(argMap.get(CommandsKey.DESTTABLES.getKey()), String.class));

        request.setProperty(CommandsKey.IS_DOWNLOADING.getKey(),
                getMappedValue(argMap.get(CommandsKey.IS_DOWNLOADING.getKey()), Boolean.class));

        request.setProperty(CommandsKey.MAX_NUMR_ETRIES.getKey(),
                getMappedValue(argMap.get(CommandsKey.MAX_NUMR_ETRIES.getKey()), Integer.class));
        request.setProperty(CommandsKey.SOURCE_TABLE.getKey(),
                getMappedValue(argMap.get(CommandsKey.SOURCE_TABLE.getKey()), String.class));

        return request;
    }

    @Override
    PropDataContext execute(PropDataContext request) {
        PropDataContext response = propDataDBService.addCommandAndWaitForComplete(request);
        Long commandId = response.getProperty(CommandsKey.COMMAND_ID.getKey(), Long.class);
        if (commandId == null) {
            throw new RuntimeException("Add command failed, there's no commandId.");
        }
        return response;
    }

    @Override
    Properties getResponseProperties(PropDataContext response) {
        Properties responseProperties = new Properties();

        Long commandId = response.getProperty(CommandsKey.COMMAND_ID.getKey(), Long.class);
        String destTables = response.getProperty(CommandsKey.DESTTABLES.getKey(), String.class);
        String commandName = response.getProperty(CommandsKey.COMMAND_NAME.getKey(), String.class);

        responseProperties.put(CommandsKey.COMMAND_ID.getKey(), commandId + "");
        responseProperties.put(CommandsKey.DESTTABLES.getKey(), destTables);
        responseProperties.put(CommandsKey.COMMAND_NAME.getKey(), commandName);

        return responseProperties;
    }

    public static void main(String[] args) {
        AbstractAction<PropDataContext, PropDataContext> action = new PropDataAddCommandAction();
        action.doMain(args);
    }

}