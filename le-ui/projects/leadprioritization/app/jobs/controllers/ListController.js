angular.module('lp.jobs', [
    'lp.jobs.status',
    'pd.navigation.pagination',
    'mainApp.core.utilities.BrowserStorageUtility',
    '720kb.tooltips'
])
.controller('JobsListCtrl', function($scope, $state, $stateParams, $http, $timeout, $interval, JobsStore, JobsService, BrowserStorageUtility, ScoreLeadEnrichmentModal) {
    $scope.expanded = {};
    $scope.statuses = {};
    $scope.cancelling = {};
    $scope.showEmptyJobsMessage = false;
    $scope.hideCreationMessage = true;
    $scope.state = $state.current.name == 'home.model.jobs' ? 'model' : 'all';
    $scope.jobs = [];

    var modelId = $scope.state == 'model' ? $stateParams.modelId : null;

    if (modelId) {
        if (!JobsStore.data.models[modelId]) {
            JobsStore.data.models[modelId] = [];
        }

        $scope.jobs = JobsStore.data.models[modelId];
    } else {
        $scope.jobs = JobsStore.data.jobs;
    }
    
    $scope.header = {
        filter: { 
            label: 'Filter By',
            unfiltered: $scope.jobs,
            filtered: $scope.jobs,
            items: [
                { label: "All", action: { } },
                { label: "Completed", action: { jobStatus: 'Completed' } },
                { label: "Pending", action: { jobStatus: 'Pending' } },
                { label: "Running", action: { jobStatus: 'Running' } },
                { label: "Failed", action: { jobStatus: "Failed" } }
            ]
        },
        sort: {
            label: 'Sort By',
            icon: 'numeric',
            order: '-',
            property: 'timestamp',
            items: [
                { label: 'Timestamp',   icon: 'numeric', property: 'timestamp' },
                { label: 'Model Name',  icon: 'alpha',   property: 'modelName' },
                { label: 'Job Type',    icon: 'alpha',   property: 'jobType' },
                { label: 'Job Status',  icon: 'alpha',   property: 'status' }
            ]
        },
        create: {
            label: 'Create Model',
            sref: 'home.models.import',
            class: 'orange-button select-label',
            icon: 'fa fa-chevron-down',
            iconclass: 'orange-button select-more',
            iconrotate: true
        }
    };

    function getAllJobs(use_cache) {
        JobsStore.getJobs(use_cache, modelId).then(function(result) {
            $scope.showEmptyJobsMessage = (($scope.jobs == null || $scope.jobs.length == 0) && !use_cache);
        });
    }
    
    var BULK_SCORING_INTERVAL = 30 * 1000,
        BULK_SCORING_ID;

    // this stuff happens only on Model Bulk Scoring page
    if (modelId) {
        getAllJobs();
        BULK_SCORING_ID = $interval(getAllJobs, BULK_SCORING_INTERVAL);
    }

    $scope.$on("JobCompleted", function() {
        $scope.succeeded = true;
        if ($scope.state == 'model') {
            $scope.successMsg = 'Success! Scoring job has completed.';
        } else {
            $scope.successMsg = 'Success! Modeling job has completed.';
        }
    });

    $scope.$on("$destroy", function() {
        $interval.cancel(BULK_SCORING_ID);
        $scope.expanded = {};
        $scope.statuses = {};
        $timeout.cancel($scope.timeoutTask);
    });

    $scope.$on("SCORING_JOB_SUCCESS", function(event, data) {
        $scope.handleJobCreationSuccess(data);
    });

    $scope.handleJobCreationSuccess = function(data) {
        if (data) {
            $scope.jobCreationSuccess = JSON.parse(data);
            $scope.hideCreationMessage = false;
            if ($scope.jobCreationSuccess) {
                if ($scope.state == "all") {
                    $scope.jobQueuedMessage = "Your model has been queued for creation."
                } else if ($scope.jobCreationSuccess == true) {
                    $scope.jobQueuedMessage = "Your scoring job has been queued";
                }
            } else {
                if ($scope.state == "all") {
                    $scope.jobQueuedMessage = "Your model has failed to start running.";
                } else {
                    $scope.jobQueuedMessage = "Your scoring job has failed to start running";
                }
            }

            $scope.timeoutTask = $timeout($scope.closeJobCreationMessage, 30000);
        }
    }

    $scope.handleRescoreClick = function($event) {
        $event.target.disabled = true;
        ScoreLeadEnrichmentModal.showRescoreModal();
    };
    
    $scope.closeJobSuccessMessage = function() {
        $scope.succeeded = false;
    };

    $scope.closeJobCreationMessage = function() {
        $scope.jobCreationSuccess = null;
        $scope.hideCreationMessage = true;
        if ($scope.state == 'model') {
            $state.go('home.model.jobs', { 'jobCreationSuccess' : null });
        } else {
            $state.go('home.jobs.status', { 'jobCreationSuccess': null });
        }
    };

    $scope.handleJobCreationSuccess($stateParams.jobCreationSuccess);
});
