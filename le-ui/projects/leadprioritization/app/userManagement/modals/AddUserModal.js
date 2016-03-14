var app = angular.module('mainApp.userManagement.modals.AddUserModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.PasswordUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.userManagement.services.UserManagementService'
]);

app.service('AddUserModal', function ($compile, $templateCache, $rootScope, $http) {
    this.show = function (emails) {
        $http.get('app/userManagement/views/AddUserView.html', { cache: $templateCache }).success(function (html) {
            
            var scope = $rootScope.$new();
            scope.emails = emails;

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

app.controller('AddUserController', function ($scope, $rootScope, _, ResourceUtility, BrowserStorageUtility, StringUtility, PasswordUtility, NavUtility, RightsUtility, UserManagementService) {
    $scope.ResourceUtility = ResourceUtility;
    // get rid of external admin per Tejas. will add it back when PLS 2.1 is released
    //$scope.levelsToSelect = [RightsUtility.accessLevel.EXTERNAL_USER.name, RightsUtility.accessLevel.EXTERNAL_ADMIN.name];
    $scope.levelsToSelect = [RightsUtility.accessLevel.EXTERNAL_USER.name];

    var currentLevel = RightsUtility.getAccessLevel(BrowserStorageUtility.getClientSession().AccessLevel);
    if (currentLevel && currentLevel.ordinal == 4) {
        $scope.levelsToSelect = _.union($scope.levelsToSelect, [
            RightsUtility.accessLevel.INTERNAL_ADMIN.name,
            RightsUtility.accessLevel.SUPER_ADMIN.name
        ]);
    } else if (currentLevel && currentLevel.ordinal == 3) {
        $scope.levelsToSelect = _.union($scope.levelsToSelect, [
            RightsUtility.accessLevel.INTERNAL_ADMIN.name,
        ]);
    }

    $scope.saveInProgress = false;
    $scope.addUserErrorMessage = "";
    $scope.showAddUserError = false;

    $scope.user = {AccessLevel: RightsUtility.accessLevel.EXTERNAL_USER.name};

    function isLatticeEmail(email) {
        var domain = 'lattice-engines.com';
        return email.substring(email.length - domain.length) === domain;
    }

    function validateNewUser() {
        if ($scope.form.$error.required) {
            $scope.addUserErrorMessage = ResourceUtility.getString("ADD_USER_REQUIERD");
            return false;
        }

        if ($scope.form.$error.email) {
            $scope.addUserErrorMessage = ResourceUtility.getString("ADD_USER_INVALID_EMAIL");
            return false;
        }

        if (_.contains($scope.emails, $scope.user.Email)) {
            $scope.addUserErrorMessage = ResourceUtility.getString("ADD_USER_CONFLICT_EMAIL");
            return false;
        }

        var targetLevel = RightsUtility.getAccessLevel($scope.user.AccessLevel);

        // only lattice email can be assigned to levels higher than INTERNAL_USER
        if (targetLevel.ordinal === 0 && isLatticeEmail($scope.user.Email)) {
            targetLevel = RightsUtility.accessLevel.INTERNAL_USER;
            $scope.user.AccessLevel = targetLevel.name;
        }

        if (targetLevel.ordinal >= 2 && !isLatticeEmail($scope.user.Email)) {
            $scope.addUserErrorMessage = ResourceUtility.getString("ADD_USER_EXTERNAL_EMAIL_INTERNAL_ROLE");
            return false;
        } else if (targetLevel.ordinal < 2 && isLatticeEmail($scope.user.Email)) {
            $scope.addUserErrorMessage = ResourceUtility.getString("ADD_USER_INTERNAL_EMAIL_EXTERNAL_ROLE");
            return false;
        }

        return true;
    }

    $scope.addUserClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        if ($scope.saveInProgress) { return; }
        $scope.saveInProgress = true;
        $scope.showAddUserError = false;
        $scope.showAddUserSuccess = false;

        if (!validateNewUser()) {
            $scope.saveInProgress = false;
            $scope.showAddUserError = true;
            $event.target.blur();
            return;
        }

        UserManagementService.AddUser($scope.user).then(function(result){
            if (result.Success) {
                $scope.showAddUserSuccess = true;
                var levelString = ResourceUtility.getString('ACCESS_LEVEL_' + result.ResultObj.AccessLevel);
                $scope.addUserSuccessMessage=ResourceUtility.getString("ADD_USER_SUCCESS", [result.ResultObj.Username, levelString]);
                $scope.saveInProgress = false;
                $scope.showExistingUser = false;
                $event.target.blur();
            } else {
                if (result.ResultObj.ConflictingUser != null) {
                    $scope.existingUser = result.ResultObj.ConflictingUser;
                    $scope.showExistingUser = true;
                } else {
                    if (result.ResultErrors === ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')) {
                        $scope.addUserErrorMessage = ResourceUtility.getString("ADD_USER_GENERAL_ERROR");
                    } else {
                        $scope.addUserErrorMessage = result.ResultErrors;
                    }
                    $scope.showAddUserSuccess = false;
                    $scope.showAddUserError = true;
                    $scope.saveInProgress = false;
                    $scope.showExistingUser = false;
                    $event.target.blur();
                }
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

    $scope.yesClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        $scope.showExistingUser = false;
        $scope.user.FirstName = $scope.existingUser.FirstName;
        $scope.user.LastName = $scope.existingUser.LastName;
        $scope.user.Email = $scope.existingUser.Email;
        $scope.user.Username = $scope.existingUser.Username;

        UserManagementService.AssignAccessLevel($scope.user.Username, $scope.user.AccessLevel).then(function(result){
            if (result.Success) {
                $scope.showAddUserSuccess = true;
                $scope.addUserSuccessMessage=ResourceUtility.getString("ADD_EXSITING_USER_SUCCESS",
                    [$scope.user.Username, ResourceUtility.getString('ACCESS_LEVEL_' + $scope.user.AccessLevel)]);
            } else {
                $scope.addUserErrorMessage = ResourceUtility.getString("ADD_USER_GENERAL_ERROR");
                $scope.showAddUserSuccess = false;
                $scope.showAddUserError = true;
            }
            $scope.saveInProgress = false;
            $event.target.blur();
        });
    };

    $scope.noClick = function () {
        $scope.saveInProgress = false;
        $scope.showExistingUser = false;
    };
});
