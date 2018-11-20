angular.module('lp.jobs.model', [
    'lp.jobs.status',
    'pd.navigation.pagination',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.services.HealthService',
    '720kb.tooltips'
])
.controller('JobsListCtrl', function($scope, $state, $stateParams, $http, $timeout, $interval, $filter,
    JobsStore, JobsService, BrowserStorageUtility, ScoreLeadEnrichmentModal, HealthService, ModelConfig, FilterService) {
    $scope.expanded = {};
    $scope.statuses = {};
    $scope.cancelling = {};
    $scope.showEmptyJobsMessage = false;
    $scope.hideCreationMessage = true;
    $scope.state = $state.current.name == 'home.model.jobs' ? 'model' : 'all';
    $scope.jobs = JobsStore.data.jobs;
    $scope.isInternalAdmin = false;
    $scope.auth = BrowserStorageUtility.getTokenDocument();
    $scope.pagesize = 10;
    $scope.currentPage = 1;


    var clientSession = BrowserStorageUtility.getClientSession();
    $scope.TenantId = clientSession.Tenant.Identifier;

    $scope.init = function() {
        if (BrowserStorageUtility.getSessionDocument() != null && BrowserStorageUtility.getSessionDocument().User != null
            && BrowserStorageUtility.getSessionDocument().User.AccessLevel != null) {
            var accessLevel = BrowserStorageUtility.getSessionDocument().User.AccessLevel;
            if (accessLevel == "INTERNAL_ADMIN" || accessLevel == "SUPER_ADMIN") {
                $scope.isInternalAdmin = true;
            }
        }

        var modelId = ModelConfig.ModelId;//$scope.state == 'model' ? $stateParams.modelId : null;

        if (modelId && modelId !== '') {
            if (!JobsStore.data.models[modelId]) {
                JobsStore.data.models[modelId] = [];
            }

            $scope.jobs = JobsStore.data.models[modelId];
        } else {
            $scope.jobs = JobsStore.data.jobs;//$filter('filter')(JobsStore.data.jobs, { jobType: '!processAnalyzeWorkflow' }, true);//JobsStore.data.dataModelJobs;
            if (JobsStore.data.isModelState) {
                $scope.jobs = [];
            }
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
                    { label: "Failed", action: { jobStatus: "Failed" } },
                    { label: "Cancelled", action: { jobStatus: "Cancelled" } }
                ]
            },
            maxperpage: {
                label: false, //'Page Size',
                //click: false,
                //class: 'white-button select-label',
                icon: 'fa fa-chevron-down',
                iconlabel: 'Page Size',
                iconclass: 'white-button',
                iconrotate: true,
                items: [
                    { label: '10 items',  icon: 'numeric', click: function() { $scope.pagesize = 10; FilterService.setFilters('jobs.list.pagesize', {pagesize: $scope.pagesize}); $scope.currentPage = 1;}},
                    { label: '25 items',  icon: 'numeric', click: function() { $scope.pagesize = 25; FilterService.setFilters('jobs.list.pagesize', {pagesize: $scope.pagesize}); $scope.currentPage = 1;}},
                    { label: '50 items',  icon: 'numeric', click: function() { $scope.pagesize = 50; FilterService.setFilters('jobs.list.pagesize', {pagesize: $scope.pagesize}); $scope.currentPage = 1;}},
                    { label: '100 items', icon: 'numeric', click: function() { $scope.pagesize = 100; FilterService.setFilters('jobs.list.pagesize', {pagesize: $scope.pagesize}); $scope.currentPage = 1;}}
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
                    { label: 'Job Type',    icon: 'alpha',   property: 'displayName' },
                    { label: 'Job Status',  icon: 'alpha',   property: 'status' }
                ]
            },
            scoring: {
                label: 'Score List',
                sref: 'home.model.scoring',
                class: 'orange-button select-label',
                icon: 'fa fa-chevron-down',
                iconclass: 'orange-button select-more',
                iconrotate: true,
                items: [
                    {
                        click: $scope.handleRescoreClick,
                        label: 'Score Training Set',
                        icon: 'fa fa fa-th-list'
                    },{
                        sref: 'home.model.scoring',
                        label: 'Score List',
                        icon: 'fa fa-file-excel-o'
                    }
                ]
            }
        };

        function getAllJobs(use_cache) {
            $scope.loadingJobs = true;
            JobsStore.getJobs(use_cache, modelId).then(function(result) {
                $scope.showEmptyJobsMessage = (($scope.jobs == null || $scope.jobs.length == 0) && !use_cache);
                $scope.loadingJobs = false;
            });
        }

        var BULK_SCORING_INTERVAL = 30 * 1000,
            BULK_SCORING_ID;

        // this stuff happens only on Model Bulk Scoring page
        // getAllJobs();Call done in the JobsRoutes.js
        getAllJobs();
        if (modelId) {
            BULK_SCORING_ID = $interval( function() {
                getAllJobs();
                }, BULK_SCORING_INTERVAL);
        }

        $scope.$on("JobCompleted", function(evt, data) {
            $scope.succeeded = true;
            if ($scope.state == 'model' || data.jobType.toUpperCase().indexOf('SCORE') > -1) {
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

        $scope.handleJobCreationSuccess($stateParams.jobCreationSuccess);

        var filterStore = FilterService.getFilters('jobs.list.pagesize');
        if(filterStore) {
            $scope.pagesize = filterStore.pagesize;
        }
    };

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
    };

    $scope.handleRescoreClick = function($event) {
        if ($event) {
            $event.target.disabled = true;
        }
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

    $scope.checkStatusBeforeScore = function() {
        HealthService.checkSystemStatus().then(function() {
            $state.go('home.model.scoring')
        });
    };

    $scope.init();
});
