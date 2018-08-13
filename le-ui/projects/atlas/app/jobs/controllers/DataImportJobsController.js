angular.module('lp.jobs.imp', [])
.controller('DataImportJobsCtrl', function($scope, $http, JobsStore, $filter) {
    // filter:{jobType:'dataProcessingWorkflow'}
    $scope.jobs = JobsStore.data.jobs;
    $scope.jobs = $filter('filter')( $scope.jobs, { jobType: 'dataProcessingWorkflow' }, true)
    $scope.loadingJobs = true;
    // JobsStore.getDataImportJobs().then(function(jobs) {
    //     $scope.loadingJobs = false;
    // });

    $scope.successMsg = null;
    $scope.errorMsg = null;
    $scope.queuedMsg = null;
    $scope.pagesize = 10;

    $scope.header = {
        filter: {
            label: 'Filter By',
            unfiltered: $scope.jobs,
            filtered: $scope.jobs,
            items: [
                { label: "All", action: { } },
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
                { label: '10 items',  icon: 'numeric', click: function() { $scope.pagesize = 10;  } },
                { label: '25 items',  icon: 'numeric', click: function() { $scope.pagesize = 25;  } },
                { label: '50 items',  icon: 'numeric', click: function() { $scope.pagesize = 50;  } },
                { label: '100 items', icon: 'numeric', click: function() { $scope.pagesize = 100; } }
            ]
        },
        sort: {
            label: 'Sort By',
            icon: 'numeric',
            order: '-',
            property: 'timestamp',
            items: [
                { label: 'Timestamp',   icon: 'numeric', property: 'timestamp' },
                { label: 'File Name',   icon: 'alpha',   property: 'fileName' },
                { label: 'Job Status',  icon: 'alpha',   property: 'status' }
            ]
        }
    };

    $scope.clearMessages = function() {
        $scope.successMsg = null;
        $scope.errorMsg = null;
        $scope.queuedMsg = null;
    };
});
