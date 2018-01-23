angular.module('lp.jobs.import', ['lp.jobs.import.row', 'lp.jobs.row.subjobs', 'lp.jobs.chevron'])
    .service('Browser', ['$window', function ($window) {

        this.IEVersion = function () {
            var sAgent = $window.navigator.userAgent;
            var Idx = sAgent.indexOf("MSIE");

            // If IE, return version number.
            if (Idx > 0)
                return parseInt(sAgent.substring(Idx + 5, sAgent.indexOf(".", Idx)));

            // If IE 11 then look for Updated user agent string.
            else if (!!navigator.userAgent.match(/Trident\/7\./)) {
                return 11;
            }
            else {
                return 0; //It is not IE
            }

        }

    }])
    .directive('onFinishRender', ['$timeout', 'Browser', function ($timeout, browser) {
        return {
            restrict: 'A',
            link: function (scope, element, attr) {
                $timeout(function () {

                    var tmp = browser.IEVersion();
                    if (tmp > 0) {
                        console.log('====== Creating msGridRow =======');
                        var nodes = element.context.childNodes;
                        var j = 1;
                        for (var i = 0; i < nodes.length; i++) {
                            var className = nodes[i].className;
                            // console.log('CLASS NAME', className);
                            if (className && className.indexOf('le-row') >= 0) {
                                nodes[i].style['msGridRow'] = j;
                                j++;
                            }
                        }
                    }
                });
            }
        }
    }])
    .controller('DataProcessingComponent', function ($q, $scope, $http, JobsStore, $filter, ModalStore) {
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
                    { label: "Failed", action: { status: "Failed" } }
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
        });

        vm.initModalWindow = function () {
            vm.config = {
                'name': "import_jobs",
                'type': 'sm',
                'title': 'Run Job',
                'titlelength': 100,
                'dischargetext': 'CANCEL',
                'dischargeaction': 'cancel',
                'confirmtext': 'Proceed',
                'confirmaction': 'proceed',
                'icon': 'ico ico-cog',
                'confirmcolor': 'blue-button',
                'showclose': false
            };

            vm.modalCallback = function (args) {
                if (vm.config.dischargeaction === args) {
                    vm.toggleModal();
                } else if (vm.config.confirmaction === args) {
                    vm.toggleModal();
                    vm.callback({ 'action': 'run' });
                }
            }
            vm.toggleModal = function () {
                var modal = ModalStore.get(vm.config.name);
                if (modal) {
                    modal.toggle();
                }
            }

            $scope.$on("$destroy", function () {
                ModalStore.remove(vm.config.name);
            });
        }

        vm.init = function () {
            // $filter('filter')(JobsStore.data.jobs, { jobType: 'processAnalyzeWorkflow' }, true);
            vm.jobs = $filter('filter')(JobsStore.data.jobs, { jobType: 'processAnalyzeWorkflow' }, true); //JobsStore.data.jobs;
            vm.jobs.forEach(function (element) {
                switch (element.jobType) {
                    case 'processAnalyzeWorkflow': {
                        element.displayName = "Data Processing & Analysis"; break;
                    }
                }
            });
            vm.header.filter.unfiltered = vm.jobs;
            vm.header.filter.filtered = vm.jobs;
            vm.initModalWindow();
        }

        this.init();

        function isOneActionCompleted(job) {
            var actions = job.actions;
            var oneCompleted = false;
            if (actions) {
                actions.forEach(function (element) {
                    if (element.jobStatus === 'Completed') {
                        oneCompleted = true;
                        return oneCompleted;
                    }
                });
            }
            return oneCompleted;
        }

        function isOneFailed() {
            var isFailed = false;
            vm.jobs.forEach(function (element) {
                if (element.jobStatus === 'Failed') {
                    isFailed = true;
                    return isFailed;
                }
            });
            return isFailed;
        }

        function isOneRunning() {
            var isOneRunning = false;
            vm.jobs.forEach(function (element) {
                if (element.jobStatus === 'Running') {
                    isOneRunning = true;
                    return isOneRunning;
                }
            });
            return isOneRunning;
        }

        vm.canLastJobRun = function () {
            var canRun = false;
            var oneFailed = isOneFailed();
            var oneRunnig = isOneRunning();
            var oneActionCompleted = false;
            vm.jobs.forEach(function (element) {
                if (isOneActionCompleted(element)) {
                    oneActionCompleted = true;
                    if (!oneFailed && !oneRunnig && oneActionCompleted) {
                        canRun = true;
                        return canRun;
                    }
                }
            });

            if (!oneFailed && !oneRunnig && oneActionCompleted) {
                canRun = true;
            }
            return canRun;
        }
        vm.showWarningRun = function (job) {
            var actions = job.actions;
            var allCompleted = true;
            if (actions) {
                for (var i = 0; i < actions.length; i++) {
                    if (actions[i].jobStatus === 'Running') {
                        allCompleted = false;
                        break;
                    }
                }
            }
            return !allCompleted;
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

        vm.clearMessages = function () {
            vm.successMsg = null;
            vm.errorMsg = null;
            vm.queuedMsg = null;
        };
    });
