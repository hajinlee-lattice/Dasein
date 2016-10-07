angular.module("app.modelquality.controller.ModelQualityNavigationCtrl", [
])
.controller('ModelQualityNavigationCtrl', function ($scope, $state) {

    var vm = this;
    angular.extend(vm, {
        labels: {
            DASHBOARD: 'Dashboard',
            CREATE_PIPELINE: 'Create Pipeline'
        }
    });

});
