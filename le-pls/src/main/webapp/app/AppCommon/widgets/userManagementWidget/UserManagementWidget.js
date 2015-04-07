angular.module('mainApp.appCommon.widgets.UserManagementWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.DateTimeFormatUtility',
    'mainApp.core.utilities.GriotNavUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.userManagement.modals.AddUserModal',
    'mainApp.userManagement.modals.DeleteUserModal',
    'mainApp.userManagement.services.UserManagementService'
])
.controller('UserManagementWidgetController', function ($scope, $rootScope, _, ResourceUtility, RightsUtility, AddUserModal, DeleteUserModal) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.deleteInProgress = false;

    if( Object.prototype.toString.call( $scope.data ) !== '[object Array]' ) {
        $scope.data = [$scope.data];
    }
    var data = $scope.data;

    $scope.mayAddUsers = RightsUtility.mayAddUsers();
    $scope.mayDeleteUsers = RightsUtility.mayDeleteUsers();
    $scope.showEditUserButton = RightsUtility.mayChangeUserAccessLevels();
    $scope.showDeleteUserButton = RightsUtility.mayDeleteUsers();

    $scope.selected = _.range(data.length).map(function () { return false; });

    $scope.addUserClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        AddUserModal.show(data.map(function(u){ return u.Email; }));
    };

    $scope.deleteUserClicked = function(user) {
        DeleteUserModal.show(user);
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
})
.directive('userManagementWidget', function () {
    return {
        templateUrl: 'app/AppCommon/widgets/userManagementWidget/UserManagementWidgetTemplate.html'
    };
});