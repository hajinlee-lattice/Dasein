angular.module('mainApp.core.utilities.RightsUtility', [])
.service('RightsUtility', function () {

    this.canViewUsers = function(rightsDict) {
        if (rightsDict.hasOwnProperty("PLS_Users")) {
            var userRights = rightsDict.PLS_Users;
            return (userRights.MayView || userRights.MayEdit);
        }
        return false;
    };

    this.canEditUsers = function(rightsDict) {
        if (rightsDict.hasOwnProperty("PLS_Users")) {
            var userRights = rightsDict.PLS_Users;
            return userRights.MayEdit;
        }
        return false;
    };

    this.canSeeUserManagement = function(rightsDict) {
        return this.canViewUsers(rightsDict);
    };

    this.canAddUser = function(rightsDict) {
        return this.canEditUsers(rightsDict);
    };

});
