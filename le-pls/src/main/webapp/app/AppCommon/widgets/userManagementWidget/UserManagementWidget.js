angular.module('mainApp.appCommon.widgets.ModelDetailsWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.DateTimeFormatUtility',
    'mainApp.core.utilities.GriotNavUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.userManagement.modals.AddUserModal',
    'mainApp.userManagement.services.UserManagementService'
])
.service('DeleteUsersModal', function ($compile, $rootScope, $http) {
    this.show = function (users) {
        $http.get('./app/AppCommon/widgets/userManagementWidget/DeleteUsersView.html').success(function (html) {

            var scope = $rootScope.$new();
            scope.users = users;

            var modalElement = $("#modalContainer");
            $compile(modalElement.html(html))(scope);

            var options = {
                backdrop: "static"
            };
            modalElement.modal(options);
            modalElement.modal('show');

            // Remove the created HTML from the DOM
            modalElement.on('hidden.bs.modal', function (evt) {
                modalElement.empty();
            });
        });
    };
})
.controller('DeleteUsersController', function ($scope, $rootScope, ResourceUtility, StringUtility, PasswordUtility, GriotNavUtility, UserManagementService) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.deleteInProgess = true;

    $scope.successUsers = [];
    $scope.failUsers = [];

    function deleteUserWithTimeout (user, timeout) {
        setTimeout(
            UserManagementService.DeleteUser(user).then(function(result){
                if (result.Success) {
                    $scope.successUsers.push(result.User);
                } else {
                    $scope.failUsers.push(result.User);
                }
                if ($scope.successUsers.length + $scope.failUsers.length == $scope.users.length) {
                    $scope.deleteInProgess = false;
                }
            }),
            timeout * 1000
        );
    }

    _.zip($scope.users, _.range($scope.users.length)).map(function(params){
        deleteUserWithTimeout(params[0], params[1]);
    });

    $scope.okClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        if (!$scope.deleteInProgess) {
            $("#modalContainer").modal('hide');
            $rootScope.$broadcast(GriotNavUtility.USER_MANAGEMENT_NAV_EVENT);
        }
    };
})
.controller('UserManagementWidgetController', function ($scope, $rootScope, _, ResourceUtility, AddUserModal, DeleteUsersModal, UserManagementService, GriotNavUtility, StringUtility) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.deleteInProgress = false;

    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    var data = $scope.data;
    $scope.showAddUserButton = metadata.mayAddUser;

    $scope.toBeDeleted = _.range(data.length).map(function () { return false; });

    $scope.deleteUsersClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        if ($scope.deleteInProgress) { return; }
        $scope.deleteInProgress = true;

        var usersToBeDeleted = _.range(data.length)
            .filter(function(i){ return $scope.toBeDeleted[i]; })
            .map(function(i){ return $scope.data[i]; });

        DeleteUsersModal.show(usersToBeDeleted);
    };

    $scope.addUserClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        AddUserModal.show();
    };
})
.directive('userManagementWidget', function ($compile) {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/userManagementWidget/UserManagementWidgetTemplate.html'
    };

    return directiveDefinitionObject;
});