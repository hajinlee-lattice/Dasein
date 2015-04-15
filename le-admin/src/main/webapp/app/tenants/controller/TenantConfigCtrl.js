var app = angular.module("app.tenants.controller.TenantConfigCtrl", [
    'app.tenants.service.TenantService',
    'app.tenants.directive.CamilleConfigDirective',
    'le.common.util.UnderscoreUtility',
    'ui.bootstrap',
    'ui.router',
    'ngSanitize'
]);

app.controller('TenantConfigCtrl', function($scope, $state, $stateParams, $modal, $interval, _, TenantService, TenantUtility) {
    $scope.readonly = true;
    $scope.listenState = true;
    if ($stateParams.readonly) $scope.readonly = $stateParams.readonly === "true";
    if ($stateParams.listenState) $scope.listenState = $stateParams.listenState === "true";

    $scope.tenantId = $stateParams.tenantId;
    $scope.contractId = $stateParams.contractId || $stateParams.tenantId;
    $scope.space = $stateParams.space || "production";
    $scope.product = $stateParams.product;

    console.log($stateParams, $scope.readonly, $scope.listenState);

    $scope.loading = true;
    $scope.services = ["PLS", "VDB"];

    $scope.accordion = _.map($scope.services, function(){
        return { open: false, disabled: false };
    });

    $scope.components = [];
    $scope.isValid = {valid: true};

    _.each($scope.services, function(service){
        TenantService.GetTenantServiceConfig($scope.tenantId, service).then(
            function(result){
                var component = result.resultObj;
                TenantService.GetServiceMetadata(service).then(
                    function(metadata){
                        TenantUtility.applyMetadataToComponent(component, metadata);
                        $scope.components.push(component);

                        if ($scope.components.length == $scope.services.length) {
                            $scope.loading = false;
                            if ($scope.listenState) { updateServiceStatus(); }
                        }
                    }
                );
            }
        );
    });

    function updateServiceStatus() {
        _.each($scope.components, function(component){
            TenantService.GetTenantServiceStatus($scope.tenantId, component.component).then(
                function(result){
                    component.State = result.state;
                    console.debug(
                        "The status of " + component.Component +
                        " is updated to " + component.State);
                }
            );
        });
    }

    //if ($scope.mode !== 'NEW') {
    //    var statusUpdater = $interval(function(){
    //        if ($state.current.name !== "TENANT.CONFIG") {
    //            $interval.cancel(statusUpdater);
    //        }
    //        if (!$scope.loading) {
    //            updateServiceStatus();
    //        }
    //    }, 5000);
    //}

    $scope.onSaveClick = function(){
        $scope.cleanData = TenantUtility.cleanupComponentConfigs($scope.components, $scope.tenantId, $scope.contractId, $scope.space);

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


