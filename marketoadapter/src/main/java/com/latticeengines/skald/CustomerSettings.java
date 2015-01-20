package com.latticeengines.skald;

public class CustomerSettings
{
    public final String customerID;
    public final String customerKey;
    public final String destination;
    public final String databaseServer;
   
    // Necessary for Jackson deserialization
    private CustomerSettings()
    {
        customerID = null;
        customerKey = null;
        destination = null;
        databaseServer = null;
    }
}
