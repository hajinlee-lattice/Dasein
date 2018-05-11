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

	// console.log(JSON.stringify(vm.job));

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


	vm.downloadReport = function() {
		var data, filename, link;
		filename = 'report' + vm.job.id + '.csv';

		JobsService.generateJobsReport(vm.jobId).then(function(result) {
			var csv = result.Result;
	        if (!csv.match(/^data:text\/csv/i)) {
	            csv = 'data:text/csv;charset=utf-8,' + csv;
	        }
	        data = encodeURI(csv);

	        link = document.createElement('a');
	        link.setAttribute('href', data);
	        link.setAttribute('download', filename);
	        link.click();
	    });
	}




	vm.init();
})
