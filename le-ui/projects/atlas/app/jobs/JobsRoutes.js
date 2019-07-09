angular
    .module("lp.jobs", [
        "lp.jobs.model",
        "lp.jobs.import",
        "lp.jobs.export",
        "lp.jobs.orphan"
    ])
    .config(function($stateProvider, $routeProvider, $httpProvider) {
        $stateProvider
            .state("home.jobs", {
                url: "/jobs",
                onExit: function(FilterService) {
                    FilterService.clear();
                },
                views: {
                    // "navigation@": {
                    //     controller: 'SidebarRootController',
                    //     templateUrl: 'app/navigation/sidebar/RootView.html'
                    // },
                    "summary@": {
                        controller: "JobsTabsController",
                        controllerAs: "vm",
                        templateUrl: "app/jobs/tabs/jobstabs.component.html"
                    }
                },
                redirectTo: "jobs.status"
            })
            .state("home.model.jobs", {
                url: "/jobs",
                params: {
                    pageIcon: "ico-scoring",
                    pageTitle: "Jobs",
                    jobCreationSuccess: null
                },
                resolve: {
                    ResourceString: function(Model) {
                        var sourceSchemaInterpretation =
                            Model.ModelDetails.SourceSchemaInterpretation;

                        if (sourceSchemaInterpretation == "SalesforceAccount") {
                            var modelType =
                                "MODEL_SCORING_ACCOUNTS_SUMMARY_HEADER";
                        } else {
                            var modelType =
                                "MODEL_SCORING_LEADS_SUMMARY_HEADER";
                        }

                        return modelType;
                    },
                    ModelConfig: function(
                        $q,
                        $rootScope,
                        Model,
                        JobsStore,
                        IsPmml
                    ) {
                        var config = {};
                        var ModelId = Model.ModelId
                            ? Model.ModelId
                            : Model.ModelDetails
                            ? Model.ModelDetails.ModelID
                            : undefined;
                        IsPmml = IsPmml || false;
                        config.ModelId = ModelId;
                        config.IsPmml = IsPmml;
                        return config;
                    },
                    InitJobs: function($q, Model, JobsStore) {
                        var deferred = $q.defer();
                        JobsStore.getJobs(false, Model.ModelId).then(function(
                            res
                        ) {
                            deferred.resolve();
                        });
                        return deferred.promise;
                    }
                },
                views: {
                    "summary@": {
                        controller: "OneLineController",
                        templateUrl: "app/navigation/summary/OneLineView.html"
                    },
                    "main@": {
                        controller: "JobsListCtrl",
                        templateUrl: "app/jobs/views/ListView.html"
                    }
                }
            })
            .state("home.jobs.status", {
                url: "/status",
                params: {
                    pageIcon: "ico-jobs",
                    pageTitle: "Jobs",
                    jobCreationSuccess: null
                },

                resolve: {
                    ModelConfig: function() {
                        return {};
                    }
                },
                views: {
                    "main@": {
                        controller: "JobsListCtrl",
                        templateUrl: "app/jobs/views/ListView.html"
                    }
                }
            })
            .state("home.jobs.data", {
                url: "/status/data",
                params: {
                    pageIcon: "ico-jobs",
                    pageTitle: "Jobs",
                    jobCreationSuccess: null
                },
                resolve: {
                    InitJobs: function($q, JobsStore) {
                        var deferred = $q.defer();
                        // if(JobsStore.isJobsEverFetched() == false){
                        JobsStore.getJobs(false).then(function(res) {
                            deferred.resolve();
                        });
                        // }else{
                        // deferred.resolve();
                        // }
                        return deferred.promise;
                    }
                },
                views: {
                    "main@": {
                        controller: "DataProcessingComponent",
                        controllerAs: "vm",
                        templateUrl:
                            "app/jobs/processing/dataprocessing.component.html"
                    }
                }
            })
            .state("home.jobs.export", {
                url: "/status/export",
                params: {
                    pageIcon: "ico-jobs",
                    pageTitle: "Jobs",
                    jobCreationSuccess: null
                },
                resolve: {
                    InitJobs: function($q, JobsStore) {
                        var deferred = $q.defer();

                        // if (JobsStore.isJobsEverFetched() == false) {
                        JobsStore.getJobs(false).then(function(res) {
                            deferred.resolve();
                        });
                        // } else {
                        // deferred.resolve();
                        // }

                        return deferred.promise;
                    }
                },
                views: {
                    "main@": {
                        controller: "ExportJobsController",
                        controllerAs: "vm",
                        templateUrl: "app/jobs/export/export.component.html"
                    }
                }
            })
            .state("home.jobs.orphan", {
                url: "/status/orphan",
                params: {
                    pageIcon: "ico-jobs",
                    pageTitle: "Jobs"
                },
                resolve: {
                    OrphanCounts: function($q, JobsStore) {
                        var deferred = $q.defer();

                        JobsStore.getOrphanCounts().then(function(res) {
                            console.log("getOrphanCounts", res);
                            deferred.resolve(res);
                        });

                        return deferred.promise;
                    }
                },
                views: {
                    "main@": "orphanExportList"
                }
            })
            .state("home.jobs.summary", {
                url: "/:jobId/summary",
                params: {
                    pageIcon: "ico-jobs",
                    pageTitle: "View Report"
                },
                resolve: {
                    InitJob: function($q, $stateParams, JobsStore) {
                        var deferred = $q.defer();

                        JobsStore.getJob($stateParams.jobId).then(function(
                            result
                        ) {
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    }
                },
                views: {
                    "main@": {
                        controller: "JobsSummaryController",
                        controllerAs: "vm",
                        templateUrl:
                            "app/jobs/report/jobreport/jobreport.component.html"
                    }
                }
            })
            .state("home.jobs.status.csv", {
                url: "/csv/:jobId",
                params: {
                    pageIcon: "ico-jobs",
                    pageTitle: "View Report",
                    inJobs: false
                },
                resolve: {
                    JobResult: function(
                        $q,
                        $stateParams,
                        JobsStore,
                        ServiceErrorUtility
                    ) {
                        var deferred = $q.defer();
                        JobsStore.getJob($stateParams.jobId).then(function(
                            result
                        ) {
                            ServiceErrorUtility.check({
                                data: result,
                                config: {
                                    headers: { ErrorDisplayMethod: "banner" }
                                }
                            });
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    },
                    ResourceString: function() {
                        return "SUMMARY_JOBS_IMPORT_CSV";
                    }
                },
                views: {
                    // "summary@": {
                    //     controller: "OneLineController",
                    //     templateUrl: "app/navigation/summary/OneLineView.html"
                    // },
                    "summary@": {
                        controller: "JobsTabsController",
                        controllerAs: "vm",
                        templateUrl: "app/jobs/tabs/jobstabs.component.html"
                    },
                    "main@": {
                        controller: "CSVReportController",
                        templateUrl: "app/create/jobreport/JobReportView.html"
                    }
                }
            });
    });
