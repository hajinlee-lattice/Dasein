package com.latticeengines.marketoadapter;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;

// TODO: This has a terrible name.
@Service
public class SettingsManager {
    public SettingsManager() throws IOException
    {
    	//TODO: get this from Camille instead
        File input = new File(customerSettingsPath);
        
        ObjectMapper serializer = new ObjectMapper();
        customerSettings = serializer.readValue(input,
                new TypeReference<List<CustomerSettings>>() {});
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
    
    public CustomerSettings getCustomerSettingsByCustomerSpace(CustomerSpace customerSpace)
    {
        for (CustomerSettings settings : customerSettings)
        {
            if (settings.customerSpace.equals(customerSpace))
            {
                return settings;
            }
        }
        
        return null;
    }

    private List<CustomerSettings> customerSettings;
    private static final String customerSettingsPath = "CustomerSettings.json";
}
