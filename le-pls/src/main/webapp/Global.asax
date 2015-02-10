<%@ Application Language="C#" %>
<%@ Import Namespace="System.Net" %>
<%@ Import Namespace="LatticeEngines.Griot.Core" %>
<%@ Import Namespace="LatticeEngines.Griot.Core.Initialization" %>
<%@ Import Namespace="LatticeEngines.DataBroker.ApplicationLog" %>
<%@ Import Namespace="LatticeEngines.DataBroker.DataRuntimeAPI.DataModel" %>
<%@ Import Namespace="LatticeEngines.DataBroker.LEObjectModel" %>
<%@ Import Namespace="LatticeEngines.DataBroker.LEObjectModel.Utilities" %>

<script runat="server">
    
    void Application_Start(object sender, EventArgs e)
    {
        EnterpriseLibraryUtilities.Initialize(); 
        UserSessionLookup.SetStandardWebLookupRule();

        DataUtilities.DefaultConnection = LEDataConnection.Logging;

        var initializer = new GriotInitialization(
            Server,
            new ComponentFunctionalitySpec(),
            LEComponentType.GriotWebSite);

        // EJC TODO: pull into this project and reinstate?
        //var thread = new BardModelPredictorMetadataSyncThread();
        //thread.Start();
    }

    void Application_End(object sender, EventArgs e)
    {
        MessageTransport.EndListeners();
    }
            
    void Application_Error(object sender, EventArgs e) 
    {
        Exception lastError = Server.GetLastError();
        if (lastError != null)
        {
            Exception ex = lastError.GetBaseException();
            LELog.Error(new LELogMessage(BaseUserSession.DefaultDomainID).SetException(ex));
            Server.ClearError();
        }
    }

    void Session_Start(object sender, EventArgs e) 
    {
        // Code that runs when a new session is started

    }

    void Session_End(object sender, EventArgs e) 
    {
        // Code that runs when a session ends. 
        // Note: The Session_End event is raised only when the sessionstate mode
        // is set to InProc in the Web.config file. If session mode is set to StateServer 
        // or SQLServer, the event is not raised.

    }
       
</script>
