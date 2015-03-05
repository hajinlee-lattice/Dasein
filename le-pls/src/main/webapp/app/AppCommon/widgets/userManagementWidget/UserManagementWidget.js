angular.module('mainApp.appCommon.widgets.UserManagementWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.DateTimeFormatUtility',
    'mainApp.core.utilities.GriotNavUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.userManagement.modals.AddUserModal',
    'mainApp.userManagement.services.UserManagementService'
])
.service('DeleteUsersModal', function ($compile, $rootScope, $http) {
    this.show = function (users, cancelCallback) {
        $http.get('./app/AppCommon/widgets/userManagementWidget/DeleteUsersView.html').success(function (html) {

            var scope = $rootScope.$new();
            scope.users = users;
            scope.cancelCallback = cancelCallback;

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
    $scope.deleteInProgess = false;
    $scope.deleteConfirmed = false;

    $scope.okClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        if (!$scope.deleteConfirmed) {
            $scope.deleteConfirmed = true;
            $scope.deleteInProgess = true;
            $scope.successUsers = [];
            $scope.failUsers = [];

            UserManagementService.DeleteUsers($scope.users).then(function(result){
                $scope.successUsers = result.SuccessUsers;
                $scope.failUsers = result.FailUsers;
                $scope.deleteInProgess = false;
            });

        } else if (!$scope.deleteInProgess) {
            $("#modalContainer").modal('hide');
            $rootScope.$broadcast(GriotNavUtility.USER_MANAGEMENT_NAV_EVENT);
        }
    };

    $scope.cancelClick = function(){
        $("#modalContainer").modal('hide');
        $scope.cancelCallback();
    };

})
.controller('UserManagementWidgetController', function ($scope, $rootScope, _, ResourceUtility, AddUserModal, DeleteUsersModal, UserManagementService, GriotNavUtility, StringUtility) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.deleteInProgress = false;

    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    var data = $scope.data;
    $scope.mayEditUsers = metadata.mayAddUser;

    $scope.selected = _.range(data.length).map(function () { return false; });

    $scope.deleteUsersClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        if (!_.some($scope.selected) || $scope.deleteInProgress) {
            $event.target.blur();
            return;
        }

        $scope.deleteInProgress = true;

        var usersToBeDeleted = _.range(data.length)
            .filter(function(i){ return $scope.selected[i]; })
            .map(function(i){ return $scope.data[i]; });

        var cancalCallback = function() {
            $scope.deleteInProgress = false;
            $scope.selected = _.range(data.length).map(function () { return false; });
        };

        DeleteUsersModal.show(usersToBeDeleted, cancalCallback);
    };

    $scope.addUserClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        AddUserModal.show(data.map(function(u){ return u.Email; }));
    };
})
.directive('userManagementWidget', function ($compile) {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/userManagementWidget/UserManagementWidgetTemplate.html'
    };

    return directiveDefinitionObject;
});