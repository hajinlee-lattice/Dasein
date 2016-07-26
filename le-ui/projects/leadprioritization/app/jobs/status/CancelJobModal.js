angular.module('pd.jobs.status.cancelmodal', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.service('CancelJobModal', function ($compile, $templateCache, $rootScope, $http, ResourceUtility, JobsService) {
    var self = this;
    this.show = function (jobId) {
        $http.get('app/jobs/status/CancelJobModalView.html', { cache: $templateCache }).success(function (html) {

            var scope = $rootScope.$new();
            scope.modelId = modelId;

            var modalElement = $("#modalContainer");
            $compile(modalElement.html(html))(scope);

            var options = {
                backdrop: "static"
            };
            modalElement.modal(options);
            modalElement.modal('show');

            // Remove the created HTML from the DOM
            modalElement.on('hidden.bs.modal', function (evt) {
                modalElement.empty();
            });
        });
    };
})
.controller('CancelJobController', function ($scope, $rootScope, $state, ResourceUtility, JobsService) {
    $scope.ResourceUtility = ResourceUtility;

    $scope.cancelJobClickConfirm = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        updateAsCancelledJob($scope.jobId);
    };

    function updateAsCancelledJob(jobId) {
        
        console.log("cancel job");
        $scope.cancelClicked = true;
        $scope.cancelling[job.id] = true;
        JobsService.cancelJob(jobId);
    }

    $scope.cancelClick = function () {
        $("#modalContainer").modal('hide');
    };
});
