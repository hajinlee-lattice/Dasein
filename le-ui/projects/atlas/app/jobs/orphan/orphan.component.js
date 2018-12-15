angular.module("lp.jobs.orphan", []).component("orphanExportList", {
    templateUrl: "app/jobs/orphan/orphan.component.html",
    bindings: {
        OrphanCounts: "<"
    },
    controller: function(
        JobsStore,
        JobsService,
        SegmentService,
        FilterService,
        Banner
    ) {
        var vm = this;
        vm.loading = false;
        vm.loadingJobs = JobsStore.data.loadingJobs;
        vm.pagesize = 10;
        vm.query = "";
        vm.currentPage = 1;
        vm.showDownloadMessage = false;
        vm.exportDisabled = {
            OrphanTransactions: false,
            OrphanContacts: false,
            UnmatchedAccount: false
        };
        vm.orphanCounts = JobsStore.data.orphanCounts;
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
                            FilterService.setFilters("jobs.orphan.pagesize", {
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
                            FilterService.setFilters("jobs.orphan.pagesize", {
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
                            FilterService.setFilters("jobs.orphan.pagesize", {
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
                            FilterService.setFilters("jobs.orphan.pagesize", {
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
            },
            orphanExport: {
                class: "white-button select-label",
                click: false,
                icon: "fa fa-chevron-down",
                iconlabel: "Export",
                iconclass: "save button white-button select-more",
                iconrotate: true,
                icondisabled: false,
                showSpinner: false,
                items: [
                    {
                        label: `Export ${
                            vm.orphanCounts["Unmatched Accounts"]
                        } Unmatched Accounts`,
                        icon: "fa fa-building-o",
                        disabledif: vm.orphanCounts["Unmatched Accounts"] === 0,
                        click: postOrphanWorkflow.bind(
                            null,
                            "UNMATCHED_ACCOUNT"
                        )
                    },
                    {
                        label: `Export ${
                            vm.orphanCounts["Orphan Contacts"]
                        } Orphaned Contacts`,
                        icon: "fa fa-briefcase",
                        disabledif: vm.orphanCounts["Orphan Contacts"] === 0,
                        click: postOrphanWorkflow.bind(null, "CONTACT")
                    },
                    {
                        label: `Export ${
                            vm.orphanCounts["Orphan Transactions"]
                        } Orphaned Product Purchases`,
                        icon: "fa fa-users",
                        disabledif:
                            vm.orphanCounts["Orphan Transactions"] === 0,
                        click: postOrphanWorkflow.bind(null, "TRANSACTION")
                    }
                ]
            }
        };

        angular.extend(vm, {
            jobs: JobsStore.getList("orphan"),
            successMsg: null,
            errorMsg: null,
            queuedMsg: null
        });

        function postOrphanWorkflow(orphanType) {
            Banner.success({
                title: "Orphaned Contacts Export In Progress",
                message:
                    "Your report request was accepted.  You will recieve an email with the download link once it is complete."
            });

            vm.exportDisabled[orphanType] = true;

            JobsService.postOrphanWorkflow(orphanType).then(res => {
                console.log("this should not fire");
                vm.exportDisabled[orphanType] = false;
                console.log(res);
            });
        }

        vm.init = function() {
            // vm.loading = true;
            // JobsStore.getJobs(false).then(function (result) {
            vm.jobs = JobsStore.getList("orphan");
            //     vm.loading = false;
            // });
            console.log("init", vm.jobs, vm);
            var filterStore = FilterService.getFilters("jobs.orphan.pagesize");
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

        vm.downloadOrphanExport = function(exportId) {
            if (exportId && exportId !== null) {
                SegmentService.DownloadExportedOrphans(exportId).then(function(
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
    }
});
