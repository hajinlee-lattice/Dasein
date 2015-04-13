var app = angular.module("app.tenants.controller.TenantConfigCtrl", [
    'app.tenants.service.TenantService',
    'app.tenants.directive.CamilleConfigDirective',
    'le.common.util.UnderscoreUtility',
    'ui.bootstrap',
    'ui.router',
    'ngSanitize'
]);

app.controller('TenantConfigCtrl', function($scope, $state, $stateParams, $modal, $interval, _, TenantService, TenantUtility) {
    $scope.mode = $stateParams.mode;
    $scope.tenantId = $stateParams.tenantId;
    $scope.contractId = $stateParams.hasOwnProperty("contractId") ? $stateParams.contractId : $stateParams.tenantId;
    $scope.space = $stateParams.hasOwnProperty("space") ? $stateParams.space : "production";

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
                var component = result.resultObj;
                TenantService.GetServiceMetadata(service).then(
                    function(metadata){
                        TenantUtility.applyMetadataToComponent(component, metadata);
                        $scope.data.push(component);

                        if ($scope.data.length == $scope.services.length) {
                            $scope.loading = false;
                            if ($scope.mode !== 'NEW') { updateServiceStatus(); }
                        }
                    }
                );
            }
        );
    });

    function updateServiceStatus() {
        _.each($scope.data, function(component){
            TenantService.GetTenantServiceStatus($scope.tenantId, component.component).then(
                function(result){
                    component.state = result.state;
                    console.debug(
                        "The status of " + component.component +
                        " is updated to " + component.state);
                }
            );
        });
    }

    if ($scope.mode !== 'NEW') {
        var statusUpdater = $interval(function(){
            if ($state.current.name !== "TENANT.CONFIG") {
                $interval.cancel(statusUpdater);
            }
            if (!$scope.loading) {
                updateServiceStatus();
            }
        }, 5000);
    }

    $scope.onSaveClick = function(){
        $scope.cleanData = TenantUtility.cleanupComponentConfigs($scope.data);

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


