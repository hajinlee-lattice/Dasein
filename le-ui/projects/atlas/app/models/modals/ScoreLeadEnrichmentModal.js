angular.module('mainApp.models.leadenrichment', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.service('ScoreLeadEnrichmentModal', function($compile, $templateCache, $rootScope, $http) {
    var self = this;
    this.showRescoreModal = function () {
        $http.get('app/models/views/ScoreLeadEnrichmentView.html', { cache: $templateCache }).success(function(html) {
            var scope = $rootScope.$new();
            scope.rescore = true;

            var modalElement = $("#modalContainer");
            $compile(modalElement.html(html))(scope);
            $("#deleteModelError").hide();

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

    this.showFileScoreModal = function(modelId, fileName, state) {
        $http.get('app/models/views/ScoreLeadEnrichmentView.html', { cache: $templateCache }).success(function(html) {
            var scope = $rootScope.$new();
            scope.rescore = false;
            scope.modelId = modelId;
            scope.fileName = fileName;
            scope.state = state;

            var modalElement = $("#modalContainer");
            $compile(modalElement.html(html))(scope);
            $("#deleteModelError").hide();

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
.controller('ScoreLeadEnrichmentController', function($scope, $rootScope, $state, $stateParams, $timeout, ResourceUtility, JobsService, ImportService, DataCloudStore) {
    var vm = this;
    $scope.ResourceUtility = ResourceUtility;
    $scope.saveInProgress = false;
    $scope.scoringFailed = false;
    $scope.noEnrichmentsSelected = false;

    angular.extend(vm, {
        rescore: $scope.rescore,
        state: $scope.state,
        enableLeadEnrichment: false,
    });

    $scope.disableLeadEnrichmentClicked = function() {
        vm.enableLeadEnrichment = false;
    };

    $scope.enableLeadEnrichmentClicked = function() {
        vm.enableLeadEnrichment = true;
    };

    $scope.scoreClicked = function($event) {
        if (vm.rescore) {
            $scope.saveInProgress = true;
            JobsService.rescoreTrainingData(vm.enableLeadEnrichment).then(function(jobResponse) {
                $scope.saveInProgress = false;
                if (jobResponse.success) {
                    $("#modalContainer").modal('hide');
                    $rootScope.$broadcast("SCORING_JOB_SUCCESS", jobResponse.success);
                } else {
                    $scope.scoringFailed = true;
                }
            });
        } else {
            $scope.saveInProgress = true;
            ImportService.StartTestingSet($scope.modelId, $scope.fileName, vm.enableLeadEnrichment).then(function(result) {
                $scope.saveInProgress = false;
                if (result.Success) {
                    $("#modalContainer").modal('hide');
                    $state.go('home.model.jobs', { 'jobCreationSuccess': result.Success });
                } else {
                    $scope.scoringFailed = true;
                }
            });
        }
    };

    $scope.cancelClicked = function(ignoreCancelState) {
        $("#modalContainer").modal('hide');

        if (vm.state && !ignoreCancelState) {
            $timeout(function() {
                $state.go(vm.state);
            }, 1)
        }
    };

    vm.init = function() {
        DataCloudStore.getSelectedCount().then(function(response){
            if(response.data === 0){
                $scope.noEnrichmentsSelected = true;
            }
        });
    }

    vm.init();
});
