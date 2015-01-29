package com.latticeengines.marketoadapter;
import com.latticeengines.domain.exposed.camille.CustomerSpace;

public class CustomerSettings
{
    public final String customerKey;
    public final String destination;
    public final String databaseServer;
    public final CustomerSpace customerSpace;
   
    // Necessary for Jackson deserialization
    private CustomerSettings()
    {
        customerKey = null;
        customerSpace = null;
        destination = null;
        databaseServer = null;
    }
}
