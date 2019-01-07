angular
    .module('common.datacloud.explorer.export.orphan', [])
    .controller('OrphanExportController', function(
        $scope, $q, $state, $stateParams, $http, 
        SegmentService, SegmentExport
    ){
        console.log(SegmentExport);
        var vm = this;
        angular.extend(vm, {
            stateParams: $stateParams,
            orphan: $stateParams.orphan,
            exportId: $stateParams.exportID,
            segmentExport: SegmentExport,
            showDownloadMessage: false,
            disableDownload: false,
            showErrorMessage: false,
        });

        vm.init = function() {
            if (!vm.isExpired()) {
                vm.downloadOrphanExport(); //automatic download
            } else {
                vm.disableDownload = true;
                vm.showErrorMessage = true;
            }
        }

        vm.isExpired = function() {
            var currentTime = Date.now();
            return currentTime > vm.segmentExport.cleanup_by;
        }

        
        vm.downloadOrphanExport = function() {
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