package com.latticeengines.skald;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

// TODO: This has a terrible name.
@Service
public class SettingsManager {
    public SettingsManager() throws IOException
    {
        File input = new File(customerSettingsPath);
        
        ObjectMapper serializer = new ObjectMapper();
        customerSettings = serializer.readValue(input,
                new TypeReference<List<CustomerSettings>>() {});
    }
    
    public CustomerSettings getCustomerSettingsByID(String customerID)
    {
        for (CustomerSettings settings : customerSettings)
        {
            if (settings.customerID.equals(customerID))
            {
                return settings;
            }
        }
        
        return null;
    }
    
    public CustomerSettings getCustomerSettingsByKey(String customerKey)
    {
        for (CustomerSettings settings : customerSettings)
        {
            if (settings.customerKey.equals(customerKey))
            {
                return settings;
            }
        }
        
        return null;
    }

    private List<CustomerSettings> customerSettings;
    private static final String customerSettingsPath = "CustomerSettings.json";
}
