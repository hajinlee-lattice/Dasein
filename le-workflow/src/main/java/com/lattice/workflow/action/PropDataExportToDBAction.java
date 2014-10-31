package com.lattice.workflow.action;

import java.util.Map;
import java.util.Properties;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.propdata.service.db.PropDataContext;
import com.latticeengines.propdata.service.db.PropDataDBService;
import com.latticeengines.propdata.service.db.PropDataKey.ImportExportKey;

public class PropDataExportToDBAction extends AbstractAction<PropDataContext, PropDataContext> {

    @Autowired
    private PropDataDBService propDataDBService;

    @Override
    PropDataContext getRequest(Map<String, String> argMap) {
        PropDataContext request = new PropDataContext();

        request.setProperty(ImportExportKey.CUSTOMER.getKey(),
                getMappedValue(argMap.get(ImportExportKey.CUSTOMER.getKey()), String.class));
        request.setProperty(ImportExportKey.TABLE.getKey(),
                getMappedValue(argMap.get(ImportExportKey.TABLE.getKey()), String.class));
        request.setProperty(ImportExportKey.MAP_COLUMN.getKey(),
                getMappedValue(argMap.get(ImportExportKey.MAP_COLUMN.getKey()), Boolean.class));

        return request;
    }

    @Override
    PropDataContext execute(PropDataContext request) {
        PropDataContext response = propDataDBService.exportToDB(request);
        Integer applicationId = response.getProperty(ImportExportKey.APPLICATION_ID.getKey(), Integer.class);
        if (applicationId == null) {
            throw new RuntimeException("Export failed, there's no applicationId.");
        }
        return response;
    }

    @Override
    Properties getResponseProperties(PropDataContext response) {
        Properties responseProperties = new Properties();
        return responseProperties;
    }

    public static void main(String[] args) {
        AbstractAction<PropDataContext, PropDataContext> action = new PropDataExportToDBAction();
        action.doMain(args);
    }

}