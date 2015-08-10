var app = angular.module('mainApp.core.utilities.RightsUtility',
    ['mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.core.utilities.BrowserStorageUtility']);

app.service('RightsUtility', function (_, BrowserStorageUtility) {

    this.accessLevel = {
        EXTERNAL_USER: {name: 'EXTERNAL_USER', ordinal: 0},
        EXTERNAL_ADMIN: {name: 'EXTERNAL_ADMIN', ordinal: 1},
        INTERNAL_USER: {name: 'INTERNAL_USER', ordinal: 2},
        INTERNAL_ADMIN: {name: 'INTERNAL_ADMIN', ordinal: 3},
        SUPER_ADMIN: {name: 'SUPER_ADMIN', ordinal: 4}
    };

    this.getAccessLevel = function(s) {
        return _.findWhere(this.accessLevel, {name : s});
    };

    this.may = function(rightsDict, operation, category) {
        if (rightsDict.hasOwnProperty("PLS_" + category)) {
            var rights = rightsDict["PLS_" + category];
            return (rights['May' + operation]);
        }
        return false;
    };

    this.currentUserMay = function(operation, category) {
        var clientSession = BrowserStorageUtility.getClientSession();
        return this.may(clientSession.AvailableRights, operation, category);
    };

    this.mayChangeModelNames = function() { return this.currentUserMay("Edit", "Models"); };
    this.mayDeleteModels = function() { return this.currentUserMay("Edit", "Models"); };
    this.mayUploadModelJson = function() { return this.currentUserMay("Create", "Models"); };

    this.mayAddUsers = function() { return this.currentUserMay("Edit", "Users"); };
    this.mayChangeUserAccessLevels = function() { return this.currentUserMay("Edit", "Users"); };
    this.mayDeleteUsers = function() { return this.currentUserMay("Edit", "Users"); };
    this.maySeeUserManagement = function() { return this.currentUserMay("View", "Users"); };

    this.maySeeAdminInfo = function() { return this.currentUserMay("View", "Reporting"); };
    this.maySeeModelCreationHistory = function() { return this.currentUserMay("View", "Reporting"); };

    this.maySeeSystemSetup = function() { return this.currentUserMay("Edit", "Configuration"); };
    this.mayEditActivateModel = function() { return this.currentUserMay("Edit", "Configuration"); };

});
