angular
    .module('common.datacloud.explorer.export', [])
    .controller('SegmentExportController', function(
        $scope, $q, $state, $stateParams, $http, 
        SegmentService, SegmentExport
    ){
        console.log(SegmentExport);
        var vm = this;
        angular.extend(vm, {
            stateParams: $stateParams,
            segment: $stateParams.segment,
            exportId: $stateParams.exportID,
            segmentExport: SegmentExport,
            showDownloadMessage: false,
            disableDownload: false,
            showErrorMessage: false,
        });

        vm.init = function() {
            var clientSession = BrowserStorageUtility.getClientSession();
            vm.tenantId = clientSession.Tenant.Identifier;
            vm.auth = BrowserStorageUtility.getTokenDocument();
            if (vm.isExpired()) {
            //     vm.downloadSegmentExport(); //automatic download
            // } else {
                vm.disableDownload = true;
                vm.showErrorMessage = true;
            }
        }

        vm.isExpired = function() {
            var currentTime = Date.now();
            return currentTime > vm.segmentExport.cleanup_by;
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