angular.module('app.modelquality.controller.PublishLatestCtrl', [
])
.controller('PublishLatestCtrl', function ($scope, $http, $q) {

    var vm = this;
    angular.extend(vm, {
        labels: {
            ALGORITHMS: 'Publish latest algorithm',
            ANALYTICPIPELINES: 'Publish latest analytic pipeline',
            PIPELINES: 'Publish latest pipeline',
            SAMPLINGCONFIGS: 'Publish latest sampling config',
            DATAFLOWS: 'Publish latest dataflow',
            PROPDATACONFIGS: 'Publish latest prop data config',
            ALL: 'Publish All'
        },
        error: false,
        message: null,
        loading: false
    });

    vm.urls = [];
    vm.urls.push({
        url: '/modelquality/algorithms/latest',
        labelKey: 'ALGORITHMS'
    });
    vm.urls.push({
        url: '/modelquality/samplingconfigs/latest',
        labelKey: 'SAMPLINGCONFIGS'
    });
    vm.urls.push({
        url: '/modelquality/dataflows/latest',
        labelKey: 'DATAFLOWS'
    });
    vm.urls.push({
        url: '/modelquality/propdataconfigs/latest',
        labelKey: 'PROPDATACONFIGS'
    });
    vm.urls.push({
        url: '/modelquality/pipelines/latest',
        labelKey: 'PIPELINES'
    });
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

    vm.publishAll = function () {
        vm.loading = true;
        vm.message = null;

        var promises = vm.urls.map(function (item) {
          return $http.post(item.url);
        });

        $q.all(promises).then(function (result) {
            vm.error = false;
            vm.message = 'POST all latest success';
        }).catch(function (error) {
            vm.error = true;
            vm.message = error.data.errorCode + ': ' + error.data.errorMsg;
        }).finally(function () {
            vm.loading = false;
        });
    };
});
