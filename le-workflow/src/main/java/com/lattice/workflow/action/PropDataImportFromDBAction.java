package com.lattice.workflow.action;

import java.util.Map;
import java.util.Properties;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.propdata.service.db.PropDataContext;
import com.latticeengines.propdata.service.db.PropDataDBService;
import com.latticeengines.propdata.service.db.PropDataKey.CommandsKey;
import com.latticeengines.propdata.service.db.PropDataKey.ImportExportKey;

public class PropDataImportFromDBAction extends AbstractAction<PropDataContext, PropDataContext> {

    @Autowired
    private PropDataDBService propDataDBService;

    @Override
    PropDataContext getRequest(Map<String, String> argMap) {
        PropDataContext request = new PropDataContext();

        request.setProperty(ImportExportKey.CUSTOMER.getKey(),
                getMappedValue(argMap.get(ImportExportKey.CUSTOMER.getKey()), String.class));
        request.setProperty(ImportExportKey.TABLE.getKey(),
                getMappedValue(argMap.get(ImportExportKey.TABLE.getKey()), String.class));
        request.setProperty(ImportExportKey.KEY_COLS.getKey(),
                getMappedValue(argMap.get(ImportExportKey.KEY_COLS.getKey()), String.class));

        request.setProperty(CommandsKey.COMMAND_ID.getKey(),
                getMappedValue(argMap.get(CommandsKey.COMMAND_ID.getKey()), Long.class));
        request.setProperty(CommandsKey.DESTTABLES.getKey(),
                getMappedValue(argMap.get(CommandsKey.DESTTABLES.getKey()), String.class));
        request.setProperty(CommandsKey.COMMAND_NAME.getKey(),
                getMappedValue(argMap.get(CommandsKey.COMMAND_NAME.getKey()), String.class));
        
        return request;
    }

    @Override
    PropDataContext execute(PropDataContext request) {
        
        PropDataContext response = propDataDBService.importFromDB(request);
        String applicationId = response.getProperty(ImportExportKey.APPLICATION_ID.getKey(), String.class);
        if (applicationId == null) {
            throw new RuntimeException("Import failed, there's no applicationId.");
        }
        return response;
    }

    @Override
    Properties getResponseProperties(PropDataContext response) {
        Properties responseProperties = new Properties();
        return responseProperties;
    }

    public static void main(String[] args) {
        AbstractAction<PropDataContext, PropDataContext> action = new PropDataImportFromDBAction();
        action.doMain(args);
    }

}