angular.module('lp.jobs.modals.cancelmodal', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.service('CancelJobModal', function ($compile, $templateCache, $rootScope, $http, ResourceUtility, JobsService) {
    var self = this;
    this.show = function (jobId, opts) {
        $http.get('app/jobs/modals/CancelJobModalView.html', { cache: $templateCache }).success(function (html) {

            var scope = $rootScope.$new();
            scope.jobId = jobId;
            scope.opts = opts || {};

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
.controller('CancelJobController', function ($scope, $rootScope, $state, ResourceUtility, JobsService,  ImportStore) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.cancelJobClickConfirm = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        updateAsCancelledJob($scope.jobId);
    };

    function updateAsCancelledJob(jobId) {
        console.log(jobId);
        JobsService.cancelJob(jobId);
        $("#modalContainer").modal('hide');
    }

    $scope.cancelClick = function () {
        $("#modalContainer").modal('hide');
    };

    $scope.resetImport = function() {
        ImportStore.ResetAdvancedSettings();
        $state.go('home.models.import');
        $("#modalContainer").modal('hide');
    };
});
