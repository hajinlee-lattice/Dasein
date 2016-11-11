angular.module('app.datacloud', [
    'app.datacloud.service.MetadataService',
    'app.datacloud.controller.MetadataCtrl',
    'app.datacloud.controller.Metadata2Ctrl'
])
.controller('DataCloudRootCtrl', function ($scope, $state, $rootScope) {

    var stateChangeStart = $rootScope.$on('$stateChangeStart', function (event, toState, toParams, fromState, fromParams) {
        if (toState.name.indexOf('DATACLOUD') === 0) {
            $scope.loading = true;
        }
    });

    var stateChangeSuccess = $rootScope.$on('$stateChangeSuccess', function(evt, toState, params) {
        $scope.loading = false;
    });

    $scope.$on('$destroy', function () {
        typeof stateChangeStart === 'function' ? stateChangeStart() : null;
        typeof stateChangeSuccess === 'function' ? stateChangeSuccess() : null;
    });

});
