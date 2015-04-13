var app = angular.module("app.tenants.controller.TenantConfigCtrl", [
    'app.tenants.service.TenantService',
    'app.tenants.directive.CamilleConfigDirective',
    'le.common.util.UnderscoreUtility',
    'ui.bootstrap',
    'ui.router',
    'ngSanitize'
]);

app.controller('TenantConfigCtrl', function($scope, $state, $stateParams, $modal, $interval, _, TenantService, TenantUtility) {
    $scope.tenantId = $stateParams.tenantId;
    $scope.loading = true;
    $scope.services = ["PLS", "VDB"];

    $scope.accordion = _.map($scope.services, function(){
        return { open: false, disabled: false };
    });

    $scope.data = [];
    $scope.isValid = {valid: true};

    _.each($scope.services, function(service){
        TenantService.GetTenantServiceConfig($scope.tenantId, service).then(
            function(result){
                TenantService.GetServiceMetadata(service).then(
                    function(metadata){
                        TenantUtility.applyMetadata(result.resultObj, metadata);
                        $scope.data.push(result.resultObj);

                        if ($scope.data.length == $scope.services.length) {
                            $scope.loading = false;
                            updateServiceStatus();
                        }
                    }
                );
            }
        );
    });

    function updateServiceStatus() {
        _.each($scope.data, function(component){
            TenantService.GetTenantServiceStatus($scope.tenantId, component.node).then(
                function(result){
                    component.state = result.state;
                    console.debug(
                        "The status of " + component.node +
                        " is updated to " + component.state);
                }
            );
        });
    }

    var statusUpdater = $interval(function(){
        if ($state.current.name !== "TENANT.CONFIG") {
            $interval.cancel(statusUpdater);
        }
        if (!$scope.loading) {
            updateServiceStatus();
        }
    }, 5000);

    $scope.getStatusHtml = function(state) {
      return TenantUtility.getStatusTemplate(state);
    };

    $scope.onSaveClick = function(){
        $scope.cleanData = TenantUtility.cleanupConfigData($scope.data);

        $modal.open({
            template: '<div class="modal-header">' +
            '<h3 class="modal-title">Data to be saved.</h3></div>' +
            '<div class="modal-body"><pre>{{ data | json }}</pre></div>' +
            '<div class="modal-footer">' +
            '<button class="btn btn-primary" ng-click="cancel()">OK</button>' +
            '<button class="btn btn-default" ng-click="cancel()">CANCEL</button>' +
            '</div>',
            controller: function($scope, $modalInstance, data){
                $scope.data = data;
                $scope.cancel = function () {
                    $modalInstance.dismiss('cancel');
                };
            },
            resolve: {
                data: function () {
                    return $scope.cleanData;
                }
            }
        });
    };
});


