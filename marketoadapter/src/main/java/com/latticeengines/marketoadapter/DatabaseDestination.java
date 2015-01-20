package com.latticeengines.marketoadapter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

@Service
public class DatabaseDestination implements RecordDestination
{
    public DatabaseDestination()
    {
        try
        {
            Class.forName("net.sourceforge.jtds.jdbc.Driver");
        }
        catch (ClassNotFoundException exc)
        {
            throw new RuntimeException("Unable to load jTDS driver class", exc);
        }
    }
    
    @Override
    public String receiveRecord(String customerID, Map<String, Object> record)
    {
        // Check that there are no special characters in the field names, because
        // those will get used as column identifiers and won't be escaped.
        for (String key : record.keySet())
        {
            if (!key.matches("^[a-zA-Z0-9_]+$"))
            {
                log.error("Received invalid field name: " + key + " for customer ID: " + customerID);
                throw new RuntimeException("Invalid field name: " + key);
            }
        }
        
        CustomerSettings settings = manager.getCustomerSettingsByID(customerID);
        
        if (record.containsKey("CustomerID"))
        {
            log.warn("Received a request that contained a CustomerID");
            throw new RuntimeException("CustomerID is not a valid field name.");
        }
        record.put("CustomerID", settings.customerID);
        
        List<String> sortedKeys = new ArrayList<String>(record.keySet());
        String names = StringUtils.collectionToCommaDelimitedString(sortedKeys);
        String values = StringUtils.collectionToCommaDelimitedString(
                Collections.nCopies(sortedKeys.size(), "?"));
        String text = "INSERT INTO " + tableName + " (" + names + ") VALUES (" + values + ")";

        // TODO: Connection pooling, performance improvements, etc.
        try (Connection connection = DriverManager.getConnection(settings.databaseServer))
        {
            PreparedStatement statement = connection.prepareStatement(text);
            for (int index = 0; index < sortedKeys.size(); index++)
            {
                statement.setObject(index + 1, record.get(sortedKeys.get(index)));
            }
            
            statement.executeUpdate();
        }
        catch (SQLException exc)
        {
            log.error("Unable to insert record into database", exc);
            
            if (exc.getErrorCode() == 207 || exc.getErrorCode() == 245)
            {
                throw new RuntimeException(exc.getMessage().replace("column",  "field"));
            }

            throw new RuntimeException("Encountered an internal system error");
        }
        
        return null;
    }
    

    @Autowired
    private SettingsManager manager;
    
    private static final String tableName = "RealtimeMarketo";
    
    private static final Log log = LogFactory.getLog(RecordDispatcher.class);
}
