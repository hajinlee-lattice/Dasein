angular.module('mainApp.appCommon.utilities.AuthenticationUtility', [
    'mainApp.appCommon.utilities.URLUtility'
])
.service('AuthenticationUtility', function (URLUtility) {
    
    this.AppendHttpHeaders = function ($http) {
        if ($http == null) {
            return;
        }
        //Default timeout
        $http.defaults.timeout = 120000;
        
        var type = URLUtility.GetSSOType();
        switch (type) {
            case URLUtility.SALESFORCE:
                if (URLUtility.Directory() != null) {
                    $http.defaults.headers.common.Directory = URLUtility.Directory();
                }
                $http.defaults.headers.common.sessionid = URLUtility.CrmSessionID();
                $http.defaults.headers.common.serverurl = URLUtility.CrmServerURL();
                $http.defaults.headers.common.userlink = URLUtility.CrmUser();
                break;

            case URLUtility.SAML:
                if (URLUtility.SamlDirectoryValue() != null) {
                    $http.defaults.headers.common.Directory = URLUtility.SamlDirectoryValue();
                }
                $http.defaults.headers.common.LESAMLUserLookup = URLUtility.SamlResponseValue();
                break;

            case URLUtility.LEA:
                if (URLUtility.Directory() != null) {
                    $http.defaults.headers.common.Directory = URLUtility.Directory();
                }
                $http.defaults.headers.common.LEAuthenticatedLookup = URLUtility.LeaResponse();
                break;
            case URLUtil.ORACLE:
                $http.defaults.headers.common.Directory = URLUtility.Directory();
                $http.defaults.headers.common.ssoToken = URLUtility.CrmSessionID();
                $http.defaults.headers.common.hostName = URLUtility.CrmServerURL();
                $http.defaults.headers.common.userlink = URLUtility.CrmUser();
                break;
        }
    };
});