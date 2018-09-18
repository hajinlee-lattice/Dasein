var app = angular.module("app.core.directive.MainNavDirective", [
    'ui.router',
    'le.common.util.BrowserStorageUtility',
    'app.core.util.SessionUtility',
]);

app.service('MainNavService', function($q, $http, SessionUtility){
    this.parseNavState = function (stateName) {
        if (stateName.indexOf("TENANT") === 0) {
            return "Tenants";
        }
        if (stateName.indexOf("MODELQUALITY") === 0) {
            return "ModelQuality";
        }
        if (stateName.indexOf("DATACLOUD") === 0) {
            return "DataCloud";
        }
        if (stateName.indexOf("METADATA") === 0) {
            return "Metadata";
        }
        return "unknown";
    };
    this.deactiveUserStatus = function(emails) {
        var defer = $q.defer();

        var result = {
            success: false,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'PUT',
            url: '/admin/internal/services/deactiveUserStatus',
            headers: {
                'MagicAuthentication': "Security through obscurity!"
            },
            data: emails
        }).success(function(data) {
            result.success = (data === "true" || data === true);
            defer.resolve(result);
        }).error(function(err, status){
            SessionUtility.handleAJAXError(err, status);
            result.errMsg = err;
        });

        return defer.promise;
    };
    this.addUserAccessLevel = function(emails, right) {
        var defer = $q.defer();

        var result = {
            success: false,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'PUT',
            url: '/admin/internal/services/addUserAccessLevel',
            headers: {
                'MagicAuthentication': "Security through obscurity!"
            },
            data: emails,
            params : { right:right }
        }).success(function(data) {
            result.success = (data === "true" || data === true);
            defer.resolve(result);
        }).error(function(err, status){
            SessionUtility.handleAJAXError(err, status);
            result.errMsg = err;
        });

        return defer.promise;
    };
    this.createHelpUser = function(userRegistration) {
        var defer = $q.defer();

        var result = {
            success: false,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'POST',
            url: '/admin/prospectingusers',
            data: userRegistration
        }).success(function(data) {
            result.success = (data === "true" || data === true);
            defer.resolve(result);
        }).error(function(err, status){
            SessionUtility.handleAJAXError(err, status);
            result.errMsg = err;
        });

        return defer.promise;
    };
});

app.directive('mainNav', function(){
    return {
        restrict: 'AE',
        templateUrl: 'app/core/view/MainNavView.html',
        scope: {activeNav: '='},
        controller: function ($scope, $rootScope, $state, $uibModal,
                              MainNavService, BrowserStorageUtility) {
            routeToCorrectState();

            $rootScope.$on('$stateChangeSuccess', function () { routeToCorrectState(); });

            $scope.onSignOutClick = function() {
                BrowserStorageUtility.clear();
                $state.go('LOGIN');
            };

            $scope.onDeactiveUserClick = function(){
                var modalInstance = $uibModal.open({
                    templateUrl: 'deactivateUsersModal.html',
                    controller: function($scope, $uibModalInstance, _, $window, MainNavService){
                        $scope.emails = "";
                        $scope.isValid = true;
                        $scope.okClicked = false;
                        $scope.saving = false;
                        $scope.errorMsg = "";

                        $scope.validateEmailInfo = function(){
                            if ($scope.addform.emailId.$error.required || $scope.emails === '') {
                                $scope.errorMsg = 'Emails is required.';
                                $scope.isValid = false;
                                return false;
                            } else {
                                $scope.errorMsg = '';
                                $scope.isValid = true;
                            }
                            return true;
                        };

                        $scope.ok = function () {
                            if ($scope.validateEmailInfo()) {
                                if ($scope.okClicked) {
                                    $scope.saving = true;

                                    MainNavService.deactiveUserStatus($scope.emails).then(function() {
                                        $uibModalInstance.close();
                                    }).catch(function(result) {
                                        $scope.saving = false;

                                        if (result && result.errMsg) {
                                            $scope.errorMsg = result.errMsg;
                                        } else {
                                            $scope.errorMsg = 'Unexpected Error. Please try again';
                                        }
                                    });
                                } else {
                                    $scope.okClicked = true;
                                }
                            }
                        };

                        $scope.cancel = function () {
                            if (!$scope.okClicked) {
                                $uibModalInstance.dismiss('cancel');
                            } else {
                                $scope.okClicked = false;
                            }
                        };
                    }
                });

                modalInstance.result.then(function () {
                    $state.go('TENANT.LIST');
                }, function () {
                    $state.go('TENANT.LIST');
                });

            };

            $scope.onAddUserAccessLevelClick = function(){
                var modalInstance = $uibModal.open({
                    templateUrl: 'addUserAccessLevelModal.html',
                    controller: function($scope, $uibModalInstance, _, $window, MainNavService){
                        $scope.emails = "";
                        $scope.isValid = true;
                        $scope.okClicked = false;
                        $scope.saving = false;
                        $scope.errorMsg = "";

                        $scope.validateEmailInfo = function(){
                            if ($scope.addform.emailId.$error.required || $scope.emails === '') {
                                $scope.errorMsg = 'Emails is required.';
                                $scope.isValid = false;
                                return false;
                            }
                            var emails = $scope.emails.trim().split(",");
                            for (var i = 0; i < emails.length; i++) {
                                var email = emails[i];
                                if(email.trim() !== '' && email.indexOf("lattice-engines") === -1) {
                                    $scope.errorMsg = 'User '+ email +' is not a lattice user. Only lattice user is allowed';
                                    $scope.isValid = false;
                                    return false;
                                }
                            }
                            $scope.errorMsg = '';
                            $scope.isValid = true;
                            return true;
                        };

                        $scope.ok = function () {
                            if ($scope.validateEmailInfo()) {
                                if ($scope.okClicked) {
                                    $scope.saving = true;

                                    MainNavService.addUserAccessLevel($scope.emails, $scope.right).then(function() {
                                        $uibModalInstance.close();
                                    }).catch(function(result) {
                                        $scope.saving = false;

                                        if (result && result.errMsg) {
                                            $scope.errorMsg = result.errMsg;
                                        } else {
                                            $scope.errorMsg = 'Unexpected Error. Please try again';
                                        }
                                    });
                                } else {
                                    $scope.okClicked = true;
                                }
                            }
                        };

                        $scope.cancel = function () {
                            if (!$scope.okClicked) {
                                $uibModalInstance.dismiss('cancel');
                            } else {
                                $scope.okClicked = false;
                            }
                        };
                    }
                });

                modalInstance.result.then(function () {
                    $state.go('TENANT.LIST');
                }, function () {
                    $state.go('TENANT.LIST');
                });

            };


            $scope.onCreateHelpUserClick = function(){
                var modalInstance = $uibModal.open({
                    templateUrl: 'createHelpUser.html',
                    controller: function($scope, $uibModalInstance, _, $window, MainNavService){
                        $scope.firstName = "";
                        $scope.lastName = "";
                        $scope.email = "";
                        $scope.isValid = true;
                        $scope.okClicked = false;
                        $scope.saving = false;
                        $scope.errorMsg = "";

                        $scope.validateInfo = function(component, displayName){
                            if ($scope.addform[component].$error.required || $scope[component] === '') {
                                $scope.errorMsg = displayName + ' is required.';
                                $scope.isValid = false;
                                return false;
                            } else {
                                $scope.errorMsg = '';
                                $scope.isValid = true;
                                return true;
                            }
                        };

                        $scope.validateAllFields = function() {
                            return $scope.validateInfo('firstName', 'First name') && $scope.validateInfo('lastName', 'Last name') && $scope.validateInfo('emailId', 'Email');
                        };

                        $scope.ok = function () {
                            if ($scope.validateAllFields()) {
                                if ($scope.okClicked) {
                                    $scope.saving = true;

                                    var userRegistration = {
                                        "Credentials": {
                                            "Password": "",
                                            "Username": $scope.email
                                        },
                                        "User": {
                                            "Email": $scope.email,
                                            "FirstName": $scope.firstName,
                                            "LastName": $scope.lastName,
                                            "Username": $scope.email
                                        }
                                    };

                                    MainNavService.createHelpUser(userRegistration).then(function(result) {
                                        console.log(result);
                                        $uibModalInstance.close();
                                    }).catch(function(result) {
                                        $scope.saving = false;

                                        if (result && result.errMsg) {
                                            $scope.errorMsg = result.errMsg;
                                        } else {
                                            $scope.errorMsg = 'Unexpected Error. Please try again';
                                        }
                                    });
                                } else {
                                    $scope.okClicked = true;
                                }
                            }
                        };

                        $scope.cancel = function () {
                            if (!$scope.okClicked) {
                                $uibModalInstance.dismiss('cancel');
                            } else {
                                $scope.okClicked = false;
                            }
                        };
                    }
                });

                modalInstance.result.then(function () {
                    $state.go('TENANT.LIST');
                }, function () {
                    $state.go('TENANT.LIST');
                });

            };

            function routeToCorrectState(){
                var loginDoc = BrowserStorageUtility.getLoginDocument();
                if(loginDoc === null){
                    BrowserStorageUtility.clear();
                    $state.go('LOGIN');
                } else {
                    $scope.username = loginDoc.Principal;
                }
                $scope.activeState = MainNavService.parseNavState($state.current.name);
            }
        }
    };
});


