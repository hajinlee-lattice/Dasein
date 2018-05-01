angular
.module('lp.jobs')
.controller('JobsSummaryController', function(
	$scope, $http, $stateParams, JobsStore, $filter, InitJob, JobsService
) {
	var vm = this;

	angular.extend(vm, {
        jobId: $stateParams.jobId,
        job: InitJob,
        systemActions: [],
        entities: [
        	'Account',
        	'Contact',
        	'Product',
        	'Transaction'
        ],
        summaries: {
        	'Account': {},
        	'Contact': {},
        	'Product': {},
        	'Transaction': {}
        },
        counts: {
        	'Account': 0,
        	'Contact': 0,
        	'Product': 0,
        	'Transaction': 0
        }
    });

	// console.log(vm.job);

	vm.init = function() {

		vm.actions = vm.job.subJobs;
		vm.reports = vm.job['reports'];
		vm.reports.forEach(function(report) {
			var payload = JSON.parse(report['json']['Payload']);
			if (report['purpose'] == 'PROCESS_ANALYZE_RECORDS_SUMMARY') {
				if (payload.EntitiesSummary) {
					vm.entities.forEach(function(entity) {
						vm.counts[entity] = payload.EntitiesSummary[entity].EntityStatsSummary ? payload.EntitiesSummary[entity].EntityStatsSummary['TOTAL'] : 0;
						vm.summaries[entity] = payload.EntitiesSummary[entity].ConsolidateRecordsSummary;
						if (entity == 'Product') {
							vm.counts[entity] = payload.EntitiesSummary[entity].ConsolidateRecordsSummary['PRODUCT_ID'] || 0;
						}
					});
				}
				if (payload['SystemActions']) {
					vm.systemActionTimestamp = report.created;
					vm.systemActions = payload['SystemActions'];
				}
			}
		});

	}

	// vm.downloadReport = function() {
 //            if (vm.exportId && vm.exportId !== null) {
 //                SegmentService.DownloadExportedSegment(vm.exportId).then(function (result) {
 //                    var contentDisposition = result.headers('Content-Disposition');
 //                    var element = document.createElement("a");
 //                    var fileName = contentDisposition.match(/filename="(.+)"/)[1];
 //                    element.download = fileName;
 //                    var file = new Blob([result.data], {type: 'application/octect-stream'});
 //                    var fileURL = window.URL.createObjectURL(file);
 //                    element.href = fileURL;
 //                    document.body.appendChild(element);
 //                    element.click();
 //                    document.body.removeChild(element);
 //                });
 //            } 

 //    }


	vm.init();
})
