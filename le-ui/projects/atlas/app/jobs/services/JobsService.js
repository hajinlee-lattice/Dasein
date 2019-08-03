angular
    .module('lp.jobs')
    .service('JobsService', function($http, $q, _, $stateParams) {
        var numStepsInGroup = {
            load_data: 0,
            match_data: 0,
            generate_insights: 0,
            create_global_model: 0,
            create_global_target_market: 0,
            score_training_set: 0
        };

        this.getErrorLog = function(JobReport) {
            var deferred = $q.defer();
            //jobType = jobType == 'importMatchAndModelWorkflow' ? 'models' : 'scores';

            $http({
                method: 'GET',
                url:
                    '/pls/fileuploads/' +
                    JobReport.name.replace('_Report', '') +
                    '/import/errors',
                headers: {
                    Accept: 'application/csv;charset=utf-8',
                    ErrorDisplayMethod: 'modal'
                }
            }).then(
                function onSuccess(response) {
                    var result = response.data;
                    if (
                        result != null &&
                        result !== '' &&
                        result.Success == true
                    ) {
                        result = response.data;
                        deferred.resolve(result);
                    } else {
                        var errors = result.Errors;
                        var result = {
                            success: false,
                            errorMsg: errors[0]
                        };
                        deferred.resolve(result.errorMsg);
                    }
                },
                function onError(response) {
                    if (!response.data) {
                        response.data = {};
                    }

                    var errorMsg =
                        response.data.errorMsg || 'unspecified error';
                    deferred.resolve(errorMsg);
                }
            );

            return deferred.promise;
        };

        this.generateJobsReport = function(jobId) {
            var deferred = $q.defer();

            $http({
                method: 'GET',
                url: '/pls/jobs/' + jobId + '/report/download'
            }).then(
                function onSuccess(response) {
                    var result = response.data;
                    if (result != null && result !== '') {
                        result = response.data;
                        deferred.resolve(result);
                    } else {
                        // var errors = result.Errors;
                        // var result = {
                        //         success: false,
                        //         errorMsg: errors[0]
                        //     };
                        // deferred.resolve(result.errorMsg);

                        if (!response.data) {
                            response.data = {};
                        }

                        var errorCode = response.data.errorCode || 'Error';
                        var errorMsg =
                            response.data.errorMsg || 'unspecified error.';

                        deferred.resolve(errorMsg);
                    }
                },
                function onError(response) {
                    if (!response.data) {
                        response.data = {};
                    }

                    var errorMsg =
                        response.data.errorMsg || 'unspecified error';
                    deferred.resolve(errorMsg);
                }
            );

            return deferred.promise;
        };

        this.getScoringResults = function(job) {
            var deferred = $q.defer();

            $http({
                method: 'GET',
                url: '/pls/scores/jobs/' + job.id + '/results/score',
                headers: {
                    Accept: 'application/csv;charset=utf-8',
                    ErrorDisplayMethod: 'banner'
                }
            }).then(
                function onSuccess(response) {
                    var result = response.data;
                    if (result != null && result !== '') {
                        result = response.data;
                        deferred.resolve(result);
                    } else {
                        // var errors = result.Errors;
                        // var result = {
                        //         success: false,
                        //         errorMsg: errors[0]
                        //     };
                        // deferred.resolve(result.errorMsg);

                        if (!response.data) {
                            response.data = {};
                        }

                        var errorCode = response.data.errorCode || 'Error';
                        var errorMsg =
                            response.data.errorMsg || 'unspecified error.';

                        deferred.resolve(errorMsg);
                    }
                },
                function onError(response) {
                    if (!response.data) {
                        response.data = {};
                    }

                    var errorMsg =
                        response.data.errorMsg || 'unspecified error';
                    deferred.resolve(errorMsg);
                }
            );

            return deferred.promise;
        };

        this.getAllJobs = function(statusFilter) {
            var deferred = $q.defer();
            var result;
            var modelId = $stateParams.modelId;
            var url = modelId ? '/pls/scores/jobs/' + modelId : '/pls/jobs';

            var params =
                modelId || !statusFilter || statusFilter.length == 0
                    ? ''
                    : '?includeEmptyPA=false&status=pending&status=running';

            $http({
                method: 'GET',
                url: url + params,
                headers: {
                    'If-Modified-Since': 0,
                    ErrorDisplayMethod: 'banner'
                }
            }).then(
                function onSuccess(response) {
                    if (response.data != null && response.data !== '') {
                        result = {
                            success: true,
                            resultObj: null
                        };

                        var jobs = _.sortBy(response.data, 'startTimestamp');
                        result.resultObj = _.map(jobs, function(job) {
                            clearNumSteps();
                            var stepRunning = getStepRunning(job);
                            var stepsCompleted = getStepsCompleted(job);
                            var stepFailed = getStepFailed(job);
                            var subJobs = getSubJobs(job);
                            var steps = getSteps(job);
                            // var actions = getActions(job);
                            // var actionsCount = getActionsCount(job)

                            return {
                                id: job.id,
                                applicationId: job.applicationId,
                                timestamp: job.startTimestamp,
                                note: job.note ? job.note : '',
                                errorCode: job.errorCode,
                                errorMsg: job.errorMsg,
                                jobType: job.jobType,
                                status: job.jobStatus,
                                inputs: job.inputs,
                                source: job.inputs
                                    ? job.inputs.SOURCE_DISPLAY_NAME
                                    : null,
                                subJobs: subJobs,
                                steps: steps,
                                user: job.user,
                                jobStatus: job.jobStatus,
                                modelName: job.inputs
                                    ? job.inputs.MODEL_DISPLAY_NAME
                                    : null,
                                modelId:
                                    job.inputs && job.inputs.MODEL_ID
                                        ? job.inputs.MODEL_ID
                                        : job.outputs && job.outputs.MODEL_ID
                                        ? job.outputs.MODEL_ID
                                        : null,
                                modelType: job.inputs
                                    ? job.inputs.MODEL_TYPE
                                    : null,
                                sourceFileExists: job.inputs
                                    ? job.inputs.SOURCE_FILE_EXISTS == 'true'
                                    : null,
                                isDeleted: job.inputs
                                    ? job.inputs.MODEL_DELETED == 'true'
                                    : null,
                                startTimestamp: job.startTimestamp,
                                endTimestamp: job.endTimestamp,
                                applicationLogUrl: job.outputs
                                    ? job.outputs.YARN_LOG_LINK_PATH
                                    : null,
                                stepRunning: stepRunning,
                                stepsCompleted: stepsCompleted,
                                stepFailed: stepFailed,
                                completedTimes: getCompletedStepTimes(
                                    job,
                                    stepRunning,
                                    stepsCompleted
                                ),
                                reports: job.reports,
                                schedulingInfo: job.schedulingInfo
                            };
                        });

                        deferred.resolve(result);
                    } else {
                        if (!result.data) {
                            result.data = {};
                        }

                        var errorCode = result.data.errorCode || 'Error';
                        var errorMsg =
                            result.data.errorMsg || 'unspecified error.';

                        deferred.resolve(errorMsg);
                    }
                },
                function onError(result) {
                    if (!result.data) {
                        result.data = {};
                    }

                    var errorMsg = result.data.errorMsg || 'unspecified error';
                    deferred.resolve(errorMsg);
                }
            );
            return deferred.promise;
        };

        this.getJobStatus = function(jobId) {
            var deferred = $q.defer();
            var result;

            if (jobId === null) {
                return;
            }

            $http({
                method: 'GET',
                url: '/pls/jobs/' + jobId,
                headers: {
                    ErrorDisplayMethod: 'none',
                    'If-Modified-Since': 0
                }
            }).then(
                function onSuccess(response) {
                    clearNumSteps();
                    var job = response.data;
                    var stepRunning = getStepRunning(job);
                    var stepsCompleted = getStepsCompleted(job);
                    var stepFailed = getStepFailed(job);
                    var subJobs = getSubJobs(job);
                    var steps = getSteps(job);
                    // var actions = getActions(job);
                    // var actionsCount = getActionsCount(job)
                    var source = null;
                    if (job.inputs !== undefined) {
                        source = job.inputs.SOURCE_DISPLAY_NAME;
                    }

                    if (
                        (stepRunning === 'generate_insights' ||
                            stepRunning === 'create_global_target_market') &&
                        stepsCompleted.indexOf('score_training_set') > -1
                    ) {
                        stepRunning = 'score_training_set';
                    } else if (
                        stepRunning === 'load_data' &&
                        stepsCompleted.indexOf('generate_insights') > -1
                    ) {
                        stepRunning = 'generate_insights';
                    }
                    result = {
                        success: true,
                        resultObj: {
                            id: job.id,
                            user: job.user,
                            // actions: actions,
                            // actionsCount: actionsCount,
                            errorCode: job.errorCode,
                            errorMsg: job.errorMsg,
                            jobType: job.jobType,
                            jobStatus: job.jobStatus,
                            status: job.jobStatus,
                            startTimestamp: job.startTimestamp,
                            stepRunning: stepRunning,
                            stepsCompleted: stepsCompleted,
                            stepFailed: stepFailed,
                            subJobs: subJobs,
                            steps: steps,
                            completedTimes: getCompletedStepTimes(
                                job,
                                stepRunning,
                                stepsCompleted
                            ),
                            reports: job.reports,
                            applicationId: job.applicationId,
                            inputs: job.inputs,
                            source: source,
                            modelName:
                                job.inputs && job.inputs.MODEL_DISPLAY_NAME
                                    ? job.inputs.MODEL_DISPLAY_NAME
                                    : null,
                            modelId:
                                job.inputs && job.inputs.MODEL_ID
                                    ? job.inputs.MODEL_ID
                                    : job.outputs && job.outputs.MODEL_ID
                                    ? job.outputs.MODEL_ID
                                    : null,
                            modelType: job.inputs
                                ? job.inputs.MODEL_TYPE
                                : null,
                            sourceFileExists: job.inputs
                                ? job.inputs.SOURCE_FILE_EXISTS == 'true'
                                : null,
                            isDeleted: job.inputs
                                ? job.inputs.MODEL_DELETED == 'true'
                                : null,
                            applicationLogUrl: job.outputs
                                ? job.outputs.YARN_LOG_LINK_PATH
                                : null
                        }
                    };

                    deferred.resolve(result);
                },
                function onError(response) {
                    if (!response.data) {
                        response.data = {};
                    }

                    var errorMsg =
                        response.data.errorMsg || 'unspecified error';
                    deferred.resolve(errorMsg);
                }
            );
            return deferred.promise;
        };

        this.getJobStatusFromApplicationId = function(applicationId) {
            var deferred = $q.defer();
            var result;

            $http({
                method: 'GET',
                url: '/pls/jobs/yarnapps/' + applicationId,
                headers: {
                    ErrorDisplayMethod: 'banner',
                    'If-Modified-Since': 0
                }
            }).then(
                function onSuccess(response) {
                    clearNumSteps();
                    var job = response.data;
                    var stepRunning = getStepRunning(job);
                    var stepsCompleted = getStepsCompleted(job);
                    var stepFailed = getStepFailed(job);

                    if (
                        (stepRunning === 'generate_insights' ||
                            stepRunning === 'create_global_target_market') &&
                        stepsCompleted.indexOf('score_training_set') > -1
                    ) {
                        stepRunning = 'score_training_set';
                    } else if (
                        stepRunning === 'load_data' &&
                        stepsCompleted.indexOf('generate_insights') > -1
                    ) {
                        stepRunning = 'generate_insights';
                    }
                    result = {
                        success: true,
                        resultObj: {
                            id: job.id,
                            user: job.user,
                            errorCode: job.errorCode,
                            errorMsg: job.errorMsg,
                            jobType: job.jobType,
                            jobStatus: job.jobStatus,
                            startTimestamp: job.startTimestamp,
                            stepRunning: stepRunning,
                            stepsCompleted: stepsCompleted,
                            stepFailed: stepFailed,
                            completedTimes: getCompletedStepTimes(
                                job,
                                stepRunning,
                                stepsCompleted
                            ),
                            reports: job.reports
                        }
                    };
                    deferred.resolve(result);
                },
                function onError(response) {
                    var errors = result.Errors;
                    var result = {
                        success: false,
                        errorMsg: errors[0]
                    };
                    deferred.resolve(result.errorMsg);
                }
            );

            return deferred.promise;
        };

        this.rescoreTrainingData = function(performEnrichment) {
            var deferred = $q.defer();
            var result = {
                success: true
            };
            var modelId = $stateParams.modelId;

            $http({
                method: 'POST',
                url: '/pls/scores/' + modelId + '/training',
                params: {
                    performEnrichment: performEnrichment,
                    useRtsApi: performEnrichment
                },
                headers: {
                    ErrorDisplayMethod: 'modal'
                }
            }).then(
                function onSuccess(response) {
                    var result = response.data;
                    deferred.resolve(result);
                },
                function onError(response) {
                    if (!response.data) {
                        response.data = {};
                    }

                    var errorMsg =
                        response.data.errorMsg || 'unspecified error';
                    deferred.resolve(errorMsg);
                }
            );

            return deferred.promise;
        };

        this.rescoreJob = function(jobId) {
            return $http({
                method: 'POST',
                url: '/pls/jobs/' + jobId + '/restart'
            });
        };

        function getCompletedStepTimes(job, runningStep, completedSteps) {
            var completedTimes = {
                load_data: null,
                match_data: null,
                generate_insights: null,
                create_global_model: null,
                create_global_target_market: null,
                score_training_set: null
            };
            var currStepIndex = 0;
            if (
                runningStep !== 'load_data' &&
                completedSteps.indexOf('load_data') > -1
            ) {
                currStepIndex += numStepsInGroup.load_data;
                completedTimes.load_data =
                    job.steps[currStepIndex - 1].endTimestamp;
            }
            if (
                runningStep !== 'match_data' &&
                completedSteps.indexOf('match_data') > -1
            ) {
                currStepIndex += numStepsInGroup.match_data;
                completedTimes.match_data =
                    job.steps[currStepIndex - 1].endTimestamp;
            }
            if (
                runningStep !== 'generate_insights' &&
                completedSteps.indexOf('generate_insights') > -1
            ) {
                currStepIndex += numStepsInGroup.generate_insights;
                completedTimes.generate_insights =
                    job.steps[currStepIndex - 1].endTimestamp;
            }
            if (
                runningStep !== 'create_global_model' &&
                completedSteps.indexOf('create_global_model') > -1
            ) {
                currStepIndex += numStepsInGroup.create_global_model;
                completedTimes.create_global_model =
                    job.steps[currStepIndex - 1].endTimestamp;
            }
            if (
                runningStep !== 'create_global_target_market' &&
                completedSteps.indexOf('create_global_target_market') > -1
            ) {
                currStepIndex += numStepsInGroup.create_global_target_market;
                completedTimes.create_global_target_market =
                    job.steps[currStepIndex - 1].endTimestamp;
            }
            if (
                runningStep !== 'score_training_set' &&
                completedSteps.indexOf('score_training_set') > -1
            ) {
                currStepIndex += numStepsInGroup.score_training_set;
                completedTimes.score_training_set =
                    job.steps[currStepIndex - 1].endTimestamp;
            }
            return completedTimes;
        }

        this.cancelJob = function(jobId) {
            return $http({
                method: 'POST',
                url: '/pls/jobs/' + jobId + '/cancel'
            });
        };

        /**
         *
         * @param {*} job
         */
        this.runJob = function(job) {
            var deferred = $q.defer();
            $http({
                method: 'POST',
                url: '/pls/cdl/processanalyze'
            }).then(
                function onSuccess(response) {
                    var result = response.data;

                    if (
                        result != null &&
                        result !== '' &&
                        result.Success == true
                    ) {
                        deferred.resolve(result);
                    } else {
                        // job.status = 'Failed';
                        var errors = result.Errors;
                        var errorResponse = {
                            success: false,
                            errorMsg: errors[0]
                        };

                        deferred.resolve(errorResponse);
                    }
                },
                function onError(response) {
                    if (!response.data) {
                        response.data = {};
                    }
                    job.status = 'Failed';

                    var errorMsg =
                        response.data.errorMsg || 'unspecified error';
                    deferred.resolve(errorMsg);
                }
            );

            return deferred.promise;
        };

        function getStepFailed(job) {
            if (job.steps) {
                for (var i = 0; i < job.steps.length; i++) {
                    if (
                        job.steps[i].stepStatus === 'Failed' ||
                        job.steps[i].stepStatus === 'Cancelled'
                    ) {
                        return job.steps[i].name;
                    }
                }
            }
            return [];
        }

        function getStepRunning(job) {
            if (job.jobStatus !== 'Running') {
                //return null;
            }

            if (!job.steps) {
                return;
            }
            for (var i = 0; i < job.steps.length; i++) {
                if (job.steps[i].stepStatus === 'Running') {
                    return job.steps[i].name;
                }
            }
            return null;
        }

        function getStepsCompleted(job) {
            if (job.steps == null) {
                return [];
            }

            var stepsCompleted = [];
            for (var i = 0; i < job.steps.length; i++) {
                if (job.steps[i].stepStatus === 'Completed') {
                    var stepCompleted = job.steps[i].name;

                    if (
                        (stepCompleted === 'generate_insights' ||
                            stepCompleted === 'create_global_target_market') &&
                        stepsCompleted.indexOf('score_training_set') > -1
                    ) {
                        numStepsInGroup.score_training_set += 1;
                        continue;
                    }

                    if (stepCompleted) {
                        numStepsInGroup[stepCompleted] += 1;
                        stepsCompleted.push(stepCompleted);
                    }
                }
            }

            return stepsCompleted;
        }

        function getSubJobs(job) {
            if (job.subJobs != undefined) {
                return job.subJobs;
            } else {
                return [];
            }
        }

        function getSteps(job) {
            if (job.steps != undefined && job.steps != null) {
                return job.steps;
            } else {
                return [];
            }
        }

        function clearNumSteps() {
            numStepsInGroup.load_data = 0;
            numStepsInGroup.match_data = 0;
            numStepsInGroup.generate_insights = 0;
            numStepsInGroup.create_global_model = 0;
            numStepsInGroup.create_global_target_market = 0;
            numStepsInGroup.score_training_set = 0;
        }

        this.getOrphanCounts = function() {
            var deferred = $q.defer();

            $http({
                url: `/pls/datacollection/orphans/count`
            }).then(res => deferred.resolve(res));

            return deferred.promise;
        };

        this.postOrphanWorkflow = function(type) {
            var deferred = $q.defer();

            console.log(`post orphan workflow ${type}`);

            // var evtSource = new EventSource(
            //     `/sse/datacollection/orphans/type/${type}/submit?Authorization=dfedf78f-9dbc-4797-990e-878bcf21251e.GbmUj4GqNB4vT7Ph&TenantId=QA_CDL_Auto_Demo_1212.QA_CDL_Auto_Demo_1212.Production&Method=POST`
            // );

            // evtSource.onopen = function(e) {
            //     console.log('-!- event onopen:', e.data, e);
            //     deferred.resolve(e);
            // };

            // evtSource.onmessage = function(e) {
            //     console.log('-!- event onmessage:', e.data, e);
            //     deferred.resolve(e);
            // };

            // evtSource.onerror = function(e) {
            //     console.log('-!- event onerror:', e.data, e);
            //     deferred.resolve(e);
            // };

            $http({
                method: 'POST',
                url: `/pls/datacollection/orphans/type/${type}/submit`
            }).then(res => {
                console.log('request end', res);
            });

            return deferred.promise;
        };
    });
