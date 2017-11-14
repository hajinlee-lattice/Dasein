angular
    .module('common.datacloud.explorer.export', [])
    .controller('SegmentExportController', function(
        $scope, $q, $state, $stateParams, $rootScope, $http, ApiHost, 
        DataCloudStore, DataCloudService, SegmentService, SegmentStore, DataCloudStore
    ){
        var vm = this;
        angular.extend(vm, {
            metadata: DataCloudStore.metadata,
            stateParams: $stateParams,
            segment: $stateParams.segment,
            exportId: $stateParams.exportID,
            segmentExport: SegmentService.GetSegmentExportByExportId(vm.exportId),
            showDownloadMessage: false
        });

        vm.init = function() {
            console.log($stateParams);
            console.log(vm);
        }

        vm.init();


        vm.downloadSegmentExport = function() {
            if (vm.exportId && vm.exportId !== null) {
                SegmentService.DownloadExportedSegment(vm.exportId).then(function (result) {
                    vm.showDownloadMessage = true;
                    console.log(result);
                });
            }

        }

        vm.hideDownloadMessage = function() {
            vm.showDownloadMessage = false;
        }
    })
