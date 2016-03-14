var app = angular.module('mainApp.userManagement.modals.EditUserModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.PasswordUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.userManagement.services.UserManagementService'
]);

app.service('EditUserModal', function ($compile, $templateCache, $rootScope, $http) {
    this.show = function (userToBeEdited) {
        $http.get('app/userManagement/views/EditUserView.html', { cache: $templateCache }).success(function (html) {
            
            var scope = $rootScope.$new();
            scope.user = userToBeEdited;

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
});


app.controller('EditUserController', function ($scope, $rootScope, _, ResourceUtility, BrowserStorageUtility, StringUtility, PasswordUtility, NavUtility, RightsUtility, UserManagementService) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.user = $scope.$parent.user;
    
    var currentLevel = RightsUtility.getAccessLevel(BrowserStorageUtility.getClientSession().AccessLevel);
    var userLevel = RightsUtility.getAccessLevel($scope.user.AccessLevel);
    
    if (userLevel.ordinal <= 1) {
        // get rid of external admin per Tejas. will add it back when PLS 2.1 is released
    	//$scope.levelsToSelect = [RightsUtility.accessLevel.EXTERNAL_USER.name, RightsUtility.accessLevel.EXTERNAL_ADMIN.name];
    	$scope.levelsToSelect = [RightsUtility.accessLevel.EXTERNAL_USER.name];
    } else if (currentLevel.ordinal == 3) {
        $scope.levelsToSelect = [RightsUtility.accessLevel.INTERNAL_USER.name, RightsUtility.accessLevel.INTERNAL_ADMIN.name];
    } else if (currentLevel.ordinal == 4) {
    	$scope.levelsToSelect = [RightsUtility.accessLevel.INTERNAL_USER.name, RightsUtility.accessLevel.INTERNAL_ADMIN.name, RightsUtility.accessLevel.SUPER_ADMIN.name];
    }

    $scope.saveInProgress = false;
    $scope.editUserErrorMessage = "";
    $scope.showEditUserError = false;
    
    $scope.targetLevel = {AccessLevel: userLevel.name};

    $scope.editUserClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        if ($scope.saveInProgress) { return; }
        $scope.saveInProgress = true;
        $scope.showEditUserError = false;
        $scope.showEditUserSuccess = false;

        UserManagementService.AssignAccessLevel($scope.user.Username, $scope.targetLevel.AccessLevel).then(function(result){
            if (result.Success) {
                $scope.showEditUserSuccess = true;
                $scope.editUserSuccessMessage=ResourceUtility.getString("EDIT_USER_SUCCESS", [result.ResultObj.Username, ResourceUtility.getString("ACCESS_LEVEL_" + result.ResultObj.AccessLevel)]);
                $scope.saveInProgress = false;
                $event.target.blur();
            } else {
                if (result.ResultErrors === ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')) {
                    $scope.editUserErrorMessage = ResourceUtility.getString("EDIT_USER_GENERAL_ERROR");
                } else {
                    $scope.editUserErrorMessage = result.ResultErrors;
                }
                $scope.showEditUserSuccess = false;
                $scope.showEditUserError = true;
                $scope.saveInProgress = false;
                $event.target.blur();
            }
        });
    };

    $scope.cancelClick = function () {
        $("#modalContainer").modal('hide');
    };

    $scope.refreshClick = function(){
        $("#modalContainer").modal('hide');
        $rootScope.$broadcast(NavUtility.USER_MANAGEMENT_NAV_EVENT);
    };
    
});
