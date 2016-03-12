angular.module('mainApp.appCommon.utilities.URLUtility', [
    'mainApp.appCommon.utilities.ConfigConstantUtility'
])
.service('URLUtility', function () {
    /**
    * Returns the location of the web server based on the main page
    */
    this.GetWebServerAddress = function (mainPage) {
        if (mainPage == null) {
            return null;
        }
        var pathname = location.pathname;
        var pageIndex = pathname.indexOf(mainPage);

        if (pageIndex != -1) {
            pathname = pathname.substring(0, pageIndex);
        }
        else {
            pathname = pathname.substring(0, pathname.length - 1);
        }

        return location.protocol + "//" + location.host + pathname;
    };
    
    this.GetBaseUrl = function () {
        return location.protocol + "//" + location.host + location.pathname;
    };
    
    this.GetUrlQueryStrings = function (url) {
        if (url == null) {
            return "";
        }
        var result = "";

        //First remove the hash if it is found
        var hashIndex = url.indexOf("#");
        if (hashIndex != -1) {
            url = url.substring(0, hashIndex);
        }

        //Find the beginning of the query string and remove everything before it
        var qIndex = url.indexOf("?");
        if (qIndex != -1) {
            result = url.substring(qIndex, url.length);
        }
        return result;
    };
    
    this.GetUrlWithQueryStrings = function (baseUrl) {
        if (baseUrl == null) {
            return null;
        }
        var url = this.HandleProtocol(baseUrl);
        var queryStringParameters = this.GetUrlQueryStrings(window.location.href);
        if (queryStringParameters != null && queryStringParameters != "?") {
            url += queryStringParameters;
        }
        return url;
    };

    this.RemoveQueryStringParameter = function (url, parameter) {
        var urlHash = "";
        var hashIndex = url.indexOf("#");
        var qIndex = url.indexOf('?');

        if (hashIndex > -1) {
            if (hashIndex < qIndex) {
                // hash is before the query string parameters
                urlHash = url.substring(hashIndex, qIndex);
            } else {
                // hash is at the end
                urlHash = url.substring(hashIndex);
                url = url.replace(urlHash, "");
            }
        }

        var urlparts = url.split('?');

        if (urlparts.length >= 2) {
            var prefix = encodeURIComponent(parameter) + '=';
            var pars = urlparts[1].split(/[&;]/g);
            for (var i = pars.length; i-- > 0; )               // reverse iteration as may be destructive
                if (pars[i].lastIndexOf(prefix, 0) !== -1)   // idiom for string.startsWith
                    pars.splice(i, 1);
            if (pars.length === 0) {
                url = urlparts[0];
            } else {
                url = urlparts[0] + '?' + pars.join('&');
            }
        }

        if (hashIndex > -1 && hashIndex < qIndex) {
            return url;
        } else {
            return url + urlHash;
        }
    };

    this.GetSalesPrismWebServerAddress = function (salesPrismURL) {
        if (salesPrismURL == null) {
            return null;
        }

        return this.HandleProtocol(salesPrismURL.replace(/\/salesprism.aspx/gi, ""));
    };

    this.GetSalesPrismMainPageAddress = function (salesPrismURL) {
        if (salesPrismURL == null) {
            return null;
        }
        var hasPage = salesPrismURL.indexOf("salesprism.aspx") !== -1;
        if (hasPage) {
            return this.HandleProtocol(salesPrismURL);
        }
        if (salesPrismURL.charAt(salesPrismURL.length - 1) === "/") {
            salesPrismURL += "salesprism.aspx";
        } else {
            salesPrismURL += "/salesprism.aspx";
        }

        return this.HandleProtocol(salesPrismURL);
    };

    this.GetBardWebServerAddress = function (bardUrl) {
        if (bardUrl == null) {
            return null;
        }

        return this.HandleProtocol(bardUrl.replace(/\/index.aspx/gi, ""));
    };

    this.GetBardMainPageAddress = function (bardUrl) {
        if (bardUrl == null) {
            return null;
        }
        var hasPage = bardUrl.indexOf("index.aspx") !== -1;
        if (hasPage) {
            return this.HandleProtocol(bardUrl);
        }
        if (bardUrl.charAt(bardUrl.length - 1) === "/") {
            bardUrl += "index.aspx";
        } else {
            bardUrl += "/index.aspx";
        }

        return this.HandleProtocol(bardUrl);
    };

    this.GetBardMainPageAddressWithHash = function (bardUrl) {
        if (bardUrl == null) {
            return null;
        }
        var currentLocation = window.location.href;
        var urlHash = "";
        var hashIndex = currentLocation.indexOf("#");
        var qIndex = currentLocation.indexOf('?');

        if (hashIndex > -1) {
            if (hashIndex < qIndex) {
                // hash is before the query string parameters
                urlHash = currentLocation.substring(hashIndex, qIndex);
            } else {
                // hash is at the end
                urlHash = currentLocation.substring(hashIndex);
            }
        }
        return this.GetBardMainPageAddress(bardUrl) + urlHash;
    };

    this.OpenWindow = function (url, location) {
        if (url == null || location == null) {
            return;
        }
        window.open(url, location);
    };

    this.OpenNewWindow = function (url) {
        this.OpenWindow(url, '_blank');
    };

    this.OpenPostWindow = function (target, url, data, fetchType) {
        var content = this.ConstructPostContent(url, data, fetchType);
        var win = window.open("about:blank", target);
        if (win) {
            win.document.write(content);
        }

        return win;
    };

    this.OpenPostIframe = function (target, url, data, fetchType) {
        var content = this.ConstructPostContent(url, data, fetchType);
        $("#" + target).html(content);
    };

    this.ConstructPostContent = function (url, data, fetchType) {
        fetchType = typeof fetchType !== 'undefined' ? fetchType : 'POST';
        var content = "<!DOCTYPE html><html><head></head><body>";
        content += "<form id=\"postForm\" method='" + fetchType + "' action='" + url + "'>";
        $.each(data, function (name, value) {
            content += "<input type='hidden' name='" + name + "' value='" + value + "'></input>";
        });
        content += "</form>";
        content += "<script type='text/javascript'>document.getElementById(\"postForm\").submit();</script>";
        content += "</body></html>";

        return content;
    };

    this.GetQueryStringValue = function (key, keepHash) {
        keepHash = typeof keepHash === 'boolean' ? keepHash : false;
        var result = null;
        var payload = window.location.href;
        var qIndex = payload.indexOf("?");
        if (qIndex != -1) {
            var queryString = payload.substring(qIndex + 1);

            var pair = null;
            var params = queryString.split("&");
            for (i = 0; i < params.length; i++) {

                pair = params[i].split("=");

                if (pair.length == 2 && pair[0] == key) {
                    result = pair[1];
                    break;
                }
            }
        }

        //cleanup: if the result contains a hash "#"
        //remove the hash and anything after
        if (result != null && !keepHash) {
            var hashIndex = result.indexOf("#");
            if (hashIndex != -1) {
                result = result.substring(0, hashIndex);
            }
        }

        return (result != null ? this.Decode(result) : null);
    };

    this.Decode = function (value) {
        var result = value;

        var matches = result.match(new RegExp("%..", "gi"));
        if (matches != null) {
            for (i = 0; i < matches.length; i++) {
                var charCode = parseInt("0x" + matches[i].substring(1));
                var character = String.fromCharCode(charCode);
                result = result.replace(matches[i], character);
            }
        }

        return result;
    };

    this.ExtractTabFromHash = function (hash) {
        var toReturn = null;
        switch (hash) {
            case ConfigConstantUtility.BardModelHash:
            case ConfigConstantUtility.BardStatusHash:
            case ConfigConstantUtility.BardConfigHash:
            case ConfigConstantUtility.BardReportHash:
                toReturn = ConfigConstantUtility.ViewBard;
                break;
            case ConfigConstantUtility.ActionCenterHash:
                toReturn = ConfigConstantUtility.ViewActionItems;
                break;
            case ConfigConstantUtility.SalesListHash:
                toReturn = ConfigConstantUtility.ViewSales;
                break;
            case ConfigConstantUtility.PlayListHash:
            case ConfigConstantUtility.LaunchMetricsHash:
            case ConfigConstantUtility.LaunchRulesHash:
            case ConfigConstantUtility.LaunchFiltersHash:
            case ConfigConstantUtility.PlayQuestionsHash:
            case ConfigConstantUtility.HoldoutsHash:
                toReturn = ConfigConstantUtility.ViewPlay;
                break;
            case ConfigConstantUtility.ConfigConsoleHash:
            case ConfigConstantUtility.AdminConsoleHash:
            case ConfigConstantUtility.UsersHash:
            case ConfigConstantUtility.UserGroupsHash:
            case ConfigConstantUtility.AlertsHash:
            case ConfigConstantUtility.PreviewAlertsHash:
            case ConfigConstantUtility.FileTemplatesHash:
            case ConfigConstantUtility.MailTemplatesHash:
            case ConfigConstantUtility.FileUploadHash:
            case ConfigConstantUtility.ExternalDataHash:
                toReturn = ConfigConstantUtility.ViewAdmin;
                break;
            case ConfigConstantUtility.InvitationsHash:
                toReturn = ConfigConstantUtility.ViewMessages;
                break;
            case ConfigConstantUtility.DashboardCenterHash:
                toReturn = ConfigConstantUtility.ViewDashboardCenter;
                break;
            case ConfigConstantUtility.ChangePasswordHash:
                toReturn = ConfigConstantUtility.ViewChangePassword;
                break;
            case ConfigConstantUtility.ProductHierarchyHash:
                toReturn = ConfigConstantUtility.ViewProductHierarchy;
                break;
            // Dante hashes     
            case ConfigConstantUtility.DantePlayHash:
                toReturn = ConfigConstantUtility.DantePlayHash;
                break;
            case ConfigConstantUtility.DantePurchaseTrendsHash:
                toReturn = ConfigConstantUtility.DantePurchaseTrendsHash;
                break;
            case ConfigConstantUtility.DanteCompanyDetailsHash:
                toReturn = ConfigConstantUtility.DanteCompanyDetailsHash;
                break;
            case ConfigConstantUtility.DanteContactsHash:
                toReturn = ConfigConstantUtility.DanteContactsHash;
                break;
        }

        return toReturn;
    };

    this.ExtractSubViewFromHash = function (hash) {
        var toReturn = null;

        switch (hash) {
            case ConfigConstantUtility.PlayListHash:
                toReturn = "Plays";
                break;
            case ConfigConstantUtility.LaunchMetricsHash:
                toReturn = "LaunchMetrics";
                break;
            case ConfigConstantUtility.LaunchRulesHash:
                toReturn = "LaunchRules";
                break;
            case ConfigConstantUtility.LaunchFiltersHash:
                toReturn = "FilterRules";
                break;
            case ConfigConstantUtility.PlayQuestionsHash:
                toReturn = "PlayQuestions";
                break;
            case ConfigConstantUtility.HoldoutsHash:
                toReturn = "UploadHoldouts";
                break;
            case ConfigConstantUtility.ConfigConsoleHash:
                toReturn = "ConfigConsole";
                break;
            case ConfigConstantUtility.AdminConsoleHash:
                toReturn = "AdminConsole";
                break;
            case ConfigConstantUtility.UsersHash:
                toReturn = "Users";
                break;
            case ConfigConstantUtility.UserGroupsHash:
                toReturn = "UserGroups";
                break;
            case ConfigConstantUtility.AlertsHash:
                toReturn = "Alerts";
                break;
            case ConfigConstantUtility.PreviewAlertsHash:
                toReturn = "PreviewAlerts";
                break;
            case ConfigConstantUtility.FileTemplatesHash:
                toReturn = "FeedTemplates";
                break;
            case ConfigConstantUtility.MailTemplatesHash:
                toReturn = "MailMerge";
                break;
            case ConfigConstantUtility.FileUploadHash:
                toReturn = "FileUpload";
                break;
            case ConfigConstantUtility.ExternalDataHash:
                toReturn = "ExternalData";
                break;
            case ConfigConstantUtility.BardStatusHash:
                toReturn = "BardStatus";
                break;
            case ConfigConstantUtility.BardConfigHash:
                toReturn = "BardConfig";
                break;
            case ConfigConstantUtility.BardReportHash:
                toReturn = "BardReport";
                break;
            case ConfigConstantUtility.BardModelHash:
                toReturn = "BardModel";
                break;
        }

        return toReturn;
    };

    this.HandleProtocol = function (url) {
        if (url == null) {
            return null;
        }
        var httpIncluded = url.indexOf("http://") === 0;
        var httpsIncluded = url.indexOf("https://") === 0;
        if (httpIncluded || httpsIncluded) {
            return url;
        } else {
            return location.protocol + "//" + url;
        }

    };

    /**
    * Query String/Navigator Property helpers 
    */

    //Different SSO query string parameters
    this.SALESFORCE = "salesforce";
    this.SAML = "saml";
    this.LEA = "lea";
    this.ORACLE = "oracle";

    this.Directory = function () {
        return this.GetQueryStringValue("Directory");
    };

    this.SamlResponseValue = function () {
        return this.GetQueryStringValue("LESAMLUserLookup");
    };

    this.SamlDirectoryValue = function () {
        return this.GetQueryStringValue("LESAMLDirectoryKey");
    };

    this.LeaResponse = function () {
        return this.GetQueryStringValue("LEAuthenticatedLookup");
    };

    /**
    * @return Current SSO type, or null if not using SSO 
    */
    this.GetSSOType = function () {
        if (this.Directory() == this.ORACLE) {
            return this.ORACLE;
        } else if (this.CrmSessionID() != null && this.CrmServerURL() != null) {
            return this.SALESFORCE;
        } else if (this.SamlResponseValue() != null) {
            return this.SAML;
        } else if (this.LeaResponse() != null) {
            return this.LEA;
        } else {
            return null;
        }
    };

    this.CrmSessionID = function () {
        var secretSessionId = this.GetQueryStringValue("sin");
        if (secretSessionId != null) {
            return secretSessionId;
        }
        return this.GetQueryStringValue("sessionid");
    };

    this.CrmServerURL = function () {
        return this.GetQueryStringValue("serverurl");
    };

    this.CrmUser = function () {
        var result = null;

        var rawValue = this.GetQueryStringValue("userlink");
        if (rawValue != null) {
            var sIndex = rawValue.lastIndexOf("/");
            if (sIndex != -1) {
                result = rawValue.substring(sIndex + 1);
            } else {
                result = rawValue;
            }
        }

        // Strip last 3 case-encoding characters
        if (result != null && result.length > 15) {
            result = result.substr(0, 15);
        }

        return result;
    };

    this.CrmType = function () {
        var crmType = this.GetQueryStringValue("crmtype");
        crmType = crmType.toLowerCase();
        crmType = StringUtil.CapitalizeFirstLetter(crmType);
        var crmTypes = ConfigConstantUtility.CrmType;
        switch (crmType) {
            case crmTypes.None:
            case crmTypes.Salesforce:
            case crmTypes.Siebel:
            case crmTypes.Oracle:
                return crmType;
            default:
                return crmTypes.None;
        }
    };

    this.CrmAccount = function () {
        return this.GetQueryStringValue("Account");
    };
    
    this.CrmLead = function () {
        return this.GetQueryStringValue("Lead");
    };
    
    this.CrmContact = function () {
        return this.GetQueryStringValue("Contact");
    };
    
    this.CrmIntegrationServer = function () {
        var result = null;

        var rawValue = this.GetQueryStringValue("userlink");
        if (rawValue != null) {
            var sIndex = rawValue.lastIndexOf("/");
            if (sIndex != -1) result = rawValue.substring(0, sIndex);
        }

        return result;
    };

    this.IntegrationServer = function () {
        var result = null;

        if (this.CrmServerURL() != null)
            result = this.CrmIntegrationServer();

        return (result != null ? result : "");
    };

    // Get the external ID of the Application's Directory
    // e.g. "default", "development", "salesforce", "saml", ...
    // code is currently based on DirectoryHelper.as: GetDirectory()
    this.GetApplicationDirectory = function () {
        var result = this.Directory();
        if (result == null) {
            result = this.SamlDirectoryValue();
        }
        return result;
    };
});