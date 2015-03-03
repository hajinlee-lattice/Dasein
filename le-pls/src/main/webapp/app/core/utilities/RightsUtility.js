angular.module('mainApp.core.utilities.RightsUtility', [])
.service('RightsUtility', function () {

    this.mayViewUsers = function(rightsDict) {
        if (rightsDict.hasOwnProperty("PLS_Users")) {
            var userRights = rightsDict.PLS_Users;
            return (userRights.MayView);
        }
        return false;
    };

    this.mayEditUsers = function(rightsDict) {
        if (rightsDict.hasOwnProperty("PLS_Users")) {
            var userRights = rightsDict.PLS_Users;
            return userRights.MayEdit;
        }
        return false;
    };

    this.mayViewModels = function(rightsDict) {
        if (rightsDict.hasOwnProperty("PLS_Models")) {
            return rightsDict.PLS_Models.MayView;
        }
        return false;
    };

    this.mayEditModels = function(rightsDict) {
        if (rightsDict.hasOwnProperty("PLS_Models")) {
            return rightsDict.PLS_Models.MayEdit;
        }
        return false;
    };

    this.mayViewConfiguration = function(rightsDict) {
        try{
            return rightsDict.PLS_Configuration.MayView;
        } catch(err) {
            return false;
        }
    };

    this.mayEditConfiguration = function(rightsDict) {
        try{
            return rightsDict.PLS_Configuration.MayEdit;
        } catch(err) {
            return false;
        }
    };

    this.mayViewReporting = function(rightsDict) {
        try{
            return rightsDict.PLS_Reporting.MayView;
        } catch(err) {
            return false;
        }
    };

    this.canSeeUserManagement = function(rightsDict) {
        return this.mayViewUsers(rightsDict);
    };

    this.mayAddUser = function(rightsDict) {
        return this.mayEditUsers(rightsDict);
    };

    this.maySeeHiddenAdminTab = function(rightsDict) {
        return (
            this.mayViewModels(rightsDict) &&
            this.mayViewConfiguration(rightsDict) &&
            this.mayViewReporting(rightsDict) &&
            this.mayEditModels &&
            this.mayEditConfiguration
        );
    };

});
