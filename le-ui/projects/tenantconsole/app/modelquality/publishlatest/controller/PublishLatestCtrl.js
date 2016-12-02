angular.module('app.modelquality.controller.PublishLatestCtrl', [
])
.controller('PublishLatestCtrl', function ($scope, $http, ModelQualityService) {

    var vm = this;
    angular.extend(vm, {
        labels: {
            NOTE: 'This will publish the latest analytic pipeline and all associated components.<br>All analytic test of type "Selected pipeline" will have production pipelines replaced.<br>All analytic test of type "Production" will have latest analytic pipeline appended.',
            PUBLISH: 'Publish'
        },
        error: false,
        message: null,
        loading: false
    });

    vm.publishLatest = function () {
        vm.loading = true;
        vm.message = null;
        vm.error = false;

        ModelQualityService.LatestAnalyticPipeline().then(function (result) {
            vm.message = 'Analytic pipeline' + result.resultObj.name + ' created';
            return ModelQualityService.UpdateAnalyticTestProduction();
        }).catch(function (error) {
            vm.error = true;

            if (error) {
                vm.message = error.errMsg.errorCode + ': ' + error.errMsg.errorMsg;
            } else {
                vm.message = 'Unexpected error has occured';
            }
        }).finally(function () {
            vm.loading = false;
        });
    };
});
