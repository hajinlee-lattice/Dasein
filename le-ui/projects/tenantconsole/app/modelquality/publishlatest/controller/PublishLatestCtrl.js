angular.module('app.modelquality.controller.PublishLatestCtrl', [
])
.controller('PublishLatestCtrl', function ($scope, $http, $q) {

    var vm = this;
    angular.extend(vm, {
        labels: {
            ANALYTICPIPELINES: 'Publish latest analytic pipeline'
        },
        error: false,
        message: null,
        loading: false
    });

    vm.urls = [];
    vm.urls.push({
        url: '/modelquality/analyticpipelines/latest',
        labelKey: 'ANALYTICPIPELINES'
    });

    vm.publishLatest = function (event,  url) {
        event.preventDefault();

        vm.loading = true;
        vm.message = null;

        $http.post(url).then(function (result) {
            vm.error = false;
            vm.message = 'POST ' + url + ' success';
        }).catch(function (error) {
            vm.error = true;
            vm.message = error.data.errorCode + ': ' + error.data.errorMsg;
        }).finally(function () {
            vm.loading = false;
        });
    };
});
