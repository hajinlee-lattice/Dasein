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
                    templateUrl: 'addNewPopModal.html',
                    controller: function($scope, $uibModalInstance, _, $window ){
                        $scope.emails = "";
                        $scope.isValid = true;

                        $scope.validateEmailInfo = function(){
                            if ($scope.addform.emailId.$error.required || $scope.emails === '') {
                                $scope.EmailIdErrorMsg = "Emails is required.";
                                $scope.showEmailIdError = true;
                                $scope.isValid = false;
                                return false;
                            }
                            else {
                                $scope.showEmailIdError = false;
                                $scope.isValid = true;
                            }
                            return true;
                        };

                        $scope.ok = function () {
                            if ($scope.validateEmailInfo()) {
                                if ($window.confirm("Please confirm deactive users?")) {
                                    $uibModalInstance.close($scope.emails);
                                }
                            }
                        };

                        $scope.cancel = function () {
                            $uibModalInstance.dismiss('cancel');
                        };
                    }
                });

                modalInstance.result.then(function (emails) {
                    MainNavService.deactiveUserStatus(emails);
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


