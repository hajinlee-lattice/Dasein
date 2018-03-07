angular.module('lp.jobs.import', [
                'lp.jobs.import.row', 
                'lp.jobs.row.subjobs', 
                'lp.jobs.chevron', 
                'mainApp.appCommon.directives.modal.window'])
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
                        var nodes = element.context.childNodes;
                        var j = 1;
                        for (var i = 0; i < nodes.length; i++) {
                            var className = nodes[i].className;
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
        vm.loading = false;
        vm.rowStatus = {};
        vm.loadingJobs = JobsStore.data.loadingJobs;
        vm.pagesize = 10;
        vm.query = '';
        vm.currentPage = 1;
        vm.header = {
            filter: {
                label: 'Filter By',
                unfiltered: JobsStore.data.importJobs,
                filtered: JobsStore.data.importJobs,
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
            jobs: JobsStore.data.importJobs,
            successMsg: null,
            errorMsg: null,
            queuedMsg: null,
        });

        vm.initModalWindow = function () {
            vm.config = {
                'name': "import_jobs",
                'type': 'sm',
                'title': 'Warning',
                'titlelength': 100,
                'dischargetext': 'Cancel',
                'dischargeaction': 'cancel',
                'confirmtext': 'Yes, Run Now',
                'confirmaction': 'proceed',
                'icon': 'fa fa-exclamation-triangle',
                'iconstyle': {'color': 'white'},
                'confirmcolor': 'blue-button',
                'showclose': true,
                'headerconfig': {'background-color':'#FDC151', 'color':'white'},
                'confirmstyle' : {'background-color':'#FDC151'}
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

            vm.rowExpanded = function(row, state){
                vm.rowStatus[row] = state;
            }

            $scope.$on("$destroy", function () {
                ModalStore.remove(vm.config.name);
            });
        }

        vm.init = function () {

            vm.loading = true;
            JobsStore.getJobs(false).then(function (result) {
                vm.jobs = JobsStore.data.importJobs;
                vm.header.filter.unfiltered = JobsStore.data.importJobs;
                vm.header.filter.filtered = JobsStore.data.importJobs;
                vm.loading = false;
            });


            vm.initModalWindow();
        }

        this.init();


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
                if (element.jobStatus === 'Running' || element.jobStatus === 'Pending') {
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

            if (!oneFailed && !oneRunnig) {
                canRun = true;
            }
            return canRun;
        }

        vm.clearMessages = function () {
            vm.successMsg = null;
            vm.errorMsg = null;
            vm.queuedMsg = null;
        };
    });
