angular
    .module("lp.jobs.export", [])
    .controller("ExportJobsController", function(
        $q,
        $scope,
        $http,
        JobsStore,
        $filter,
        SegmentService,
        FilterService
    ) {
        var vm = this;
        vm.loading = false;
        vm.loadingJobs = JobsStore.data.loadingJobs;
        vm.pagesize = 10;
        vm.query = "";
        vm.currentPage = 1;
        vm.showDownloadMessage = false;
        vm.header = {
            maxperpage: {
                label: false,
                icon: "fa fa-chevron-down",
                iconlabel: "Page Size",
                iconclass: "white-button",
                iconrotate: true,
                items: [
                    {
                        label: "10 items",
                        icon: "numeric",
                        click: function() {
                            vm.pagesize = 10;
                            vm.currentPage = 1;
                            FilterService.setFilters("jobs.export.pagesize", {
                                pagesize: vm.pagesize
                            });
                        }
                    },
                    {
                        label: "25 items",
                        icon: "numeric",
                        click: function() {
                            vm.pagesize = 25;
                            vm.currentPage = 1;
                            FilterService.setFilters("jobs.export.pagesize", {
                                pagesize: vm.pagesize
                            });
                        }
                    },
                    {
                        label: "50 items",
                        icon: "numeric",
                        click: function() {
                            vm.pagesize = 50;
                            vm.currentPage = 1;
                            FilterService.setFilters("jobs.export.pagesize", {
                                pagesize: vm.pagesize
                            });
                        }
                    },
                    {
                        label: "100 items",
                        icon: "numeric",
                        click: function() {
                            vm.pagesize = 100;
                            vm.currentPage = 1;
                            FilterService.setFilters("jobs.export.pagesize", {
                                pagesize: vm.pagesize
                            });
                        }
                    }
                ]
            },
            sort: {
                label: "Sort By",
                icon: "numeric",
                order: "-",
                property: "timestamp",
                items: [
                    {
                        label: "Time stamp",
                        icon: "numeric",
                        property: "startTimestamp"
                    },
                    // { label: 'Segment Name', icon: 'alpha', property: 'fileName' },
                    { label: "Job Status", icon: "alpha", property: "status" }
                ]
            }
        };

        angular.extend(vm, {
            jobs: JobsStore.getList("export"),
            successMsg: null,
            errorMsg: null,
            queuedMsg: null
        });

        vm.init = function() {
            // vm.loading = true;
            // JobsStore.getJobs(false).then(function (result) {
            vm.jobs = JobsStore.getList("export");
            //     vm.loading = false;
            // });

            var clientSession = BrowserStorageUtility.getClientSession();
            vm.tenantId = clientSession.Tenant.Identifier;
            vm.auth = BrowserStorageUtility.getTokenDocument();

            var filterStore = FilterService.getFilters("jobs.export.pagesize");
            if (filterStore) {
                vm.pagesize = filterStore.pagesize;
            }
        };

        this.init();

        vm.isExpired = function(job) {
            var currentTime = Date.now();
            return "EXPIRE_BY_UTC_TIMESTAMP" in job.inputs
                ? currentTime > job.inputs["EXPIRE_BY_UTC_TIMESTAMP"]
                : false;
        };

        vm.downloadSegmentExport = function(jobId) {
            if (jobId && jobId !== null) {
                SegmentService.DownloadExportedSegment(jobId).then(function(
                    result
                ) {
                    var contentDisposition = result.headers(
                        "Content-Disposition"
                    );
                    var element = document.createElement("a");
                    var fileName = contentDisposition.match(
                        /filename="(.+)"/
                    )[1];
                    element.download = fileName;


                    var file = new Blob([result.data], {
                        type: "application/octect-stream"
                    });
                    var fileURL = window.URL.createObjectURL(file);
                    element.href = fileURL;
                    document.body.appendChild(element);
                    element.click();
                    document.body.removeChild(element);
                    vm.showDownloadMessage = true;
                });
            }
        };

        vm.getStatus = function(job) {
            switch (job.jobStatus) {
                case "Failed":
                    return "Failed";
                case "Pending":
                case "Running":
                    return "In Progress";
                case "Completed":
                    if (vm.isExpired(job)) {
                        return "Expired";
                    }
            }
        };

        vm.hideDownloadMessage = function() {
            vm.showDownloadMessage = false;
        };

        vm.clearMessages = function() {
            vm.successMsg = null;
            vm.errorMsg = null;
            vm.queuedMsg = null;
        };
    });
