angular.module('mainApp.appCommon.widgets.UserManagementWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.DateTimeFormatUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.core.services.FeatureFlagService',
    'mainApp.userManagement.modals.AddUserModal',
    'mainApp.userManagement.modals.DeleteUserModal',
    'mainApp.userManagement.modals.EditUserModal',
    'mainApp.userManagement.services.UserManagementService',
    'mainApp.core.utilities.BrowserStorageUtility'
])
.controller('UserManagementWidgetController', function ($scope, $rootScope, _, ResourceUtility, BrowserStorageUtility, RightsUtility, FeatureFlagService, AddUserModal, DeleteUserModal, EditUserModal) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.deleteInProgress = false;

    if( Object.prototype.toString.call( $scope.data ) !== '[object Array]' ) {
        $scope.data = [$scope.data];
    }
    $scope.users = _.sortBy(_.sortBy($scope.data, 'Email'), function(u){
        var accessLevel = RightsUtility.getAccessLevel(u.AccessLevel);
        return accessLevel != null ? accessLevel.ordinal : 0;
    });

    var flags = FeatureFlagService.Flags();
    $scope.mayAddUsers = FeatureFlagService.FlagIsEnabled(flags.ADD_USER);
    $scope.mayDeleteUsers = FeatureFlagService.FlagIsEnabled(flags.DELETE_USER);
    $scope.showEditUserButton = FeatureFlagService.FlagIsEnabled(flags.CHANGE_USER_ACCESS);
    $scope.showDeleteUserButton = FeatureFlagService.FlagIsEnabled(flags.DELETE_USER);

    var currentLevel = RightsUtility.getAccessLevel(BrowserStorageUtility.getClientSession().AccessLevel);

    $scope.addUserClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        
        AddUserModal.show($scope.users.map(function(u){ return u.Email; }));
    };

    $scope.deleteUserClicked = function(user) {
        DeleteUserModal.show(user);
    };
    
    $scope.editUserClicked = function(user) {
        EditUserModal.show(user);
    };
    
    $scope.manipulateAccessLevel = function(accessLevel) {
        var prefix = "ACCESS_LEVEL_";
        var toReturn = null;
        if (accessLevel == null) {
                toReturn = "NA";
        } else {
                toReturn = ResourceUtility.getString(prefix + accessLevel);
        }
        return toReturn;
    };
    
    $scope.showEditButton = function(user, number) {
    	var userLevel = RightsUtility.getAccessLevel(user.AccessLevel);
    	if (currentLevel.ordinal == 3 && userLevel.ordinal == 4) {
    		return false;
    	}
    	return number == 1? $scope.showEditUserButton : $scope.showDeleteUserButton;
    };
})
.directive('userManagementWidget', function () {
    return {
        templateUrl: 'app/AppCommon/widgets/userManagementWidget/UserManagementWidgetTemplate.html'
    };
});