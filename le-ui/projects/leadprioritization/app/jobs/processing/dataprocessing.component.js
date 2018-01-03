angular.module('lp.jobs')
    .controller('DataProcessingComponent', function ($scope, $http, JobsStore, $filter) {
        var vm = this;
        vm.loadingJobs = JobsStore.data.loadingJobs;
        vm.pagesize = 10;
        vm.query = '';
        vm.header = {
            filter: {
                label: 'Filter By',
                unfiltered: [],
                filtered: [],
                unfiltered: [],
                filtered: [],
                items: [
                    { label: "All", action: {} },
                    { label: "Completed", action: { status: 'Completed' } },
                    { label: "Pending", action: { status: 'Pending' } },
                    { label: "Running", action: { status: 'Running' } },
                    { label: "Failed", action: { status: "Failed" } },
                    { label: "Cancelled", action: { status: "Cancelled" } }
                ]
            },
            maxperpage: {
                label: false,
                icon: 'fa fa-chevron-down',
                iconlabel: 'Page Size',
                iconclass: 'white-button',
                iconrotate: true,
                items: [
                    { label: '10 items', icon: 'numeric', click: function () { vm.pagesize = 10; } },
                    { label: '25 items', icon: 'numeric', click: function () { vm.pagesize = 25; } },
                    { label: '50 items', icon: 'numeric', click: function () { vm.pagesize = 50; } },
                    { label: '100 items', icon: 'numeric', click: function () { vm.pagesize = 100; } }
                ]
            },
            sort: {
                label: 'Sort By',
                icon: 'numeric',
                order: '-',
                property: 'timestamp',
                items: [
                    { label: 'Timestamp', icon: 'numeric', property: 'timestamp' },
                    { label: 'File Name', icon: 'alpha', property: 'fileName' },
                    { label: 'Job Status', icon: 'alpha', property: 'status' }
                ]
            }
        }
        angular.extend(vm, {
            jobs: [],
            successMsg: null,
            errorMsg: null,
            queuedMsg: null,
            runningJob: {}
        });


        vm.init = function () {
            $filter('filter')(JobsStore.data.jobs, { jobType: 'processAnalyzeWorkflow' }, true);
            vm.jobs = $filter('filter')(JobsStore.data.jobs, { jobType: 'processAnalyzeWorkflow' }, true); //JobsStore.data.jobs;
            // vm.JobsStore.data.jobs | jobfilter: 'processAnalyzeWorkflow')
            vm.header.filter.unfiltered = vm.jobs;
            vm.header.filter.filtered = vm.jobs;

        }

        this.init();


        vm.canLastJobRun = function () {
            var canRun = true;
            if (this.jobs.length >= 2 && this.jobs[0].status === 'Failed') {
                canRun = false;
            }

            return canRun;
        }

        vm.showRunButton = function (job) {
            if (job.status === 'Pending') {
                return true;
            } else {
                return false;
            }
        }

        vm.showReport = function (job) {

            if (job.status === 'Completed' || job.status === 'Failed') {
                return true;
            } else {
                return false;
            }
        }

        vm.isJobPending = function (job) {
            if (job.jobStatus === 'Pending') {
                return true;
            } else {
                return false;
            }

        }
        vm.isJobCompleted = function (job) {
            if ('Completed' === job.status) {
                return true;
            } else {
                return true;
            }
        }

        vm.isJobFailed = function (job) {
            if (job.status === 'Failed') {
                return true;
            } else {
                return false;
            }
        }
        vm.isJobRunning = function (job) {
            if (job.status === 'Running') {
                return true;
            } else {
                return false;
            }
        }

        vm.runJob = function (job) {
            job.status = 'Running';
            JobsStore.runJob(job).then(function (updatedJob) {
                vm.runningJob = updatedJob;
            });
        }

        vm.clearMessages = function () {
            vm.successMsg = null;
            vm.errorMsg = null;
            vm.queuedMsg = null;
        };
    });
