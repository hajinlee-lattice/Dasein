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
            vm.downloadSegmentExport(); //automatic download
        }

        


        vm.downloadSegmentExport = function() {
            if (vm.exportId && vm.exportId !== null) {
                SegmentService.DownloadExportedSegment(vm.exportId).then(function (result) {
                    var contentDisposition = result.headers('Content-Disposition');
                    var element = document.createElement("a");
                    var fileName = contentDisposition.match(/filename="(.+)"/)[1];
                    element.download = fileName;
                    var file = new Blob([result.data], {type: 'application/octect-stream'});
                    var fileURL = window.URL.createObjectURL(file);
                    element.href = fileURL;
                    document.body.appendChild(element);
                    element.click();
                    document.body.removeChild(element);
                    vm.showDownloadMessage = true;
                });
            } 

        }

        vm.hideDownloadMessage = function() {
            vm.showDownloadMessage = false;
        }

        vm.init();
    })