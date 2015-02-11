angular.module('mainApp.appCommon.utilities.EvergageUtility', [])                                                                                                                                                                        
.service('EvergageUtility', function () {
    // This will create a global variable used for Evergage tracking and 
    // add the Evergage tracking JavaScript
    this.Initialize = function (options) {
        if (options == null) {
            return;
        }
        var dataset = this.GetEnvironment(window.location.hostname);
        var company = this.GetDeploymentName(window.location.pathname);
        var evergageAccount = 'latticeengines';
        var userName = company + "-";
        
        // Add title if available
        if (options.title != null) {
            userName += options.title + "-";
        }
        
        if (options.userID != null) {
            userName += options.userID;
        } else {
            userName += Math.floor((Math.random()*1000)+1);
        }
        
        if (options.datasetPrefix != null) {
            dataset = options.datasetPrefix + dataset;
        }
        
        // _aaq is created on the salesprism.aspx page so it becomes a global variable.
        // This is required by Evergage and has to be name _aaq.
        _aaq.push(['setEvergageAccount', evergageAccount], 
                  ['setDataset', dataset], 
                  ['setUseSiteConfig', true],
                  ['setUser', userName],
                  ['setCompany', company],
                  ['setAccountType', 'Standard'],
                  ['setLoggingLevel', 'NONE']);

        var d = document, g = d.createElement('script'), s = d.getElementsByTagName('script')[0];
        g.type = 'text/javascript'; g.defer = true; g.async = true;
        g.src = document.location.protocol + '//cdn.evergage.com/beacon/' +
                evergageAccount + '/' + dataset + '/scripts/evergage.min.js';
        s.parentNode.insertBefore(g, s);
    };
    
    // Determine if instance is Production, DEP or Development
    this.GetEnvironment = function (hostName) {
        var toReturn = "development";
        if (hostName != null && hostName.toLowerCase().indexOf("clients") != -1) {
            toReturn = "production";
        } else if (hostName != null && hostName.toLowerCase().indexOf("dep") != -1) {
            toReturn = "deployment";
        }
        return toReturn;
    };
    
    // Return the Deployment name from the URL pathname in window.location
    this.GetDeploymentName = function (pathname) {
        var toReturn = 'DEV';
        if (pathname != null) {
            var lastSlashIndex = pathname.lastIndexOf("/");
            if (lastSlashIndex > 0) {
                toReturn = pathname.substring(1, lastSlashIndex);
            } else {
                toReturn = pathname.substring(1);
            }
        }
        return toReturn;
    };
    
    this.TrackAction = function (actionName) {
        if (_aaq != null && actionName != null) {
            _aaq.push(['trackAction', actionName]);
        }
    };
});