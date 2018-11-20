angular.module('lp.jobs.modals.cancelmodal', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.service('CancelJobModal', function ($compile, $templateCache, $rootScope, $http, ResourceUtility, JobsService) {
    var self = this;
    this.show = function (jobId, opts, callback) {
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
.controller('CancelJobController', function ($scope, $state, $rootScope, ResourceUtility, JobsService,  ImportStore, JobsStore) {
    
    $scope.ResourceUtility = ResourceUtility;
        
    $scope.cancelJobClickConfirm = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        $("#modalContainer").modal('hide');
        JobsService.cancelJob($scope.jobId).then(function (result) {
            JobsStore.cancelledJobs[$scope.opts.ratingId] = $scope.jobId;
            delete JobsStore.inProgressModelJobs[$scope.opts.ratingId];
            $rootScope.$broadcast("updateAsCancelledJob", $scope.jobId);
        });
    };

    $scope.cancelClick = function () {
        $("#modalContainer").modal('hide');
    };

    $scope.resetImport = function() {
        ImportStore.ResetAdvancedSettings();
        $state.go('home.models.import');
        $("#modalContainer").modal('hide');
    };

});
