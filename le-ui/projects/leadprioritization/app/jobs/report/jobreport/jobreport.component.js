angular
.module('lp.jobs')
.controller('JobsSummaryController', function(
	$scope, $http, $stateParams, JobsStore, $filter, ModalStore, InitJob, JobsService
) {
	var vm = this;

	angular.extend(vm, {
        jobId: $stateParams.jobId,
        job: InitJob,
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

	console.log(vm.job);

	vm.init = function() {

		vm.actions = vm.job.subJobs;
		console.log(vm.actions);
		vm.reports = vm.job['reports'];
		vm.reports.forEach(function(report) {
			var payload = JSON.parse(report['json']['Payload']);
			if (report['purpose'] == 'PUBLISH_DATA_SUMMARY') {
				if (payload['BucketedAccount']) {
					vm.counts['Account'] = payload['BucketedAccount'];
				}
				if (payload['SortedProduct']) {
					vm.counts['Product'] = payload['SortedProduct'];
				}
				if (payload['SortedContact']) {
					vm.counts['Contact'] = payload['SortedContact'];
				}
				if (payload['AggregatedTransaction']) {
					vm.counts['Transaction'] = payload['AggregatedTransaction'];
				}
			}
			if (report['purpose'] == 'CONSOLIDATE_RECORDS_SUMMARY') {
				vm.summaries['Account'] = payload['Account'];
				vm.summaries['Contact'] = payload['Contact'];
				vm.summaries['Product'] = payload['Product'];
				vm.summaries['Transaction'] = payload['Transaction'];
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
