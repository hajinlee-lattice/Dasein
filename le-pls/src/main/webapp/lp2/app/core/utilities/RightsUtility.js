var app = angular.module('mainApp.core.utilities.RightsUtility',
    ['mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.core.utilities.BrowserStorageUtility']);

app.service('RightsUtility', function (_, BrowserStorageUtility) {

    this.accessLevel = {
        THIRD_PARTY_USER: {name: 'THIRD_PARTY_USER', ordinal: 0},
        EXTERNAL_USER: {name: 'EXTERNAL_USER', ordinal: 1},
        EXTERNAL_ADMIN: {name: 'EXTERNAL_ADMIN', ordinal: 2},
        INTERNAL_USER: {name: 'INTERNAL_USER', ordinal: 3},
        INTERNAL_ADMIN: {name: 'INTERNAL_ADMIN', ordinal: 4},
        SUPER_ADMIN: {name: 'SUPER_ADMIN', ordinal: 5}
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

});
