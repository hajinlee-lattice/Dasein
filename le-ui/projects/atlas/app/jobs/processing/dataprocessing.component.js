angular
	.module("lp.jobs.import", [
		"lp.jobs.import.row",
		"lp.jobs.row.subjobs",
		"lp.jobs.chevron",
		"common.modal",
		"common.utilities.browserstorage"
	])
	.service("Browser", [
		"$window",
		function($window) {
			this.IEVersion = function() {
				var sAgent = $window.navigator.userAgent;
				var Idx = sAgent.indexOf("MSIE");

				// If IE, return version number.
				if (Idx > 0)
					return parseInt(
						sAgent.substring(Idx + 5, sAgent.indexOf(".", Idx))
					);
				// If IE 11 then look for Updated user agent string.
				else if (!!navigator.userAgent.match(/Trident\/7\./)) {
					return 11;
				} else {
					return 0; //It is not IE
				}
			};
		}
	])
	.directive("onFinishRender", [
		"$timeout",
		"Browser",
		function($timeout, browser) {
			return {
				restrict: "A",
				link: function(scope, element, attr) {
					$timeout(function() {
						var tmp = browser.IEVersion();
						if (tmp > 0) {
							var nodes = element.context.childNodes;
							var j = 1;
							for (var i = 0; i < nodes.length; i++) {
								var className = nodes[i].className;
								if (
									className &&
									className.indexOf("le-row") >= 0
								) {
									nodes[i].style["msGridRow"] = j;
									j++;
								}
							}
						}
					});
				}
			};
		}
	])
	.filter("jobEmpty", [
		"AuthorizationUtility",
		function(AuthorizationUtility) {
			var right = AuthorizationUtility.checkAccessLevel([
				"INTERNAL_ADMIN",
				"SUPER_ADMIN",
				"EXTERNAL_ADMIN"
			]);
			return function(jobs) {
				var ret = jobs.filter(function(job) {
					if (
						(job &&
							job.id == 0 &&
							job.subJobs.length == 0 &&
							right) ||
						job.id != 0 ||
						job.subJobs.length > 0
					) {
						return job;
					}
				});
				return ret;
			};
		}
	])
	.controller("DataProcessingComponent", function(
		$q,
		$scope,
		$http,
		JobsStore,
		$filter,
		Modal,
		FilterService
	) {
		var vm = this;
		vm.loading = false;
		vm.rowStatus = {};
		vm.loadingJobs = JobsStore.data.loadingJobs;
		vm.pagesize = 10;
		vm.query = "";
		vm.currentPage = 1;
		vm.header = {
			filter: {
				label: "Filter By",
				unfiltered: JobsStore.getList("import"),
				filtered: JobsStore.getList("import"),
				items: [
					{ label: "All", action: {} },
					{ label: "Completed", action: { status: "Completed" } },
					{ label: "Pending", action: { status: "Pending" } },
					{ label: "Ready", action: { status: "Ready" } },
					{ label: "Running", action: { status: "Running" } },
					{ label: "Failed", action: { status: "Failed" } }
				]
			},
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
							FilterService.setFilters(
								"jobs.dataprocessing.pagesize",
								{ pagesize: vm.pagesize }
							);
							vm.currentPage = 1;
						}
					},
					{
						label: "25 items",
						icon: "numeric",
						click: function() {
							vm.pagesize = 25;
							FilterService.setFilters(
								"jobs.dataprocessing.pagesize",
								{ pagesize: vm.pagesize }
							);
							vm.currentPage = 1;
						}
					},
					{
						label: "50 items",
						icon: "numeric",
						click: function() {
							vm.pagesize = 50;
							FilterService.setFilters(
								"jobs.dataprocessing.pagesize",
								{ pagesize: vm.pagesize }
							);
							vm.currentPage = 1;
						}
					},
					{
						label: "100 items",
						icon: "numeric",
						click: function() {
							vm.pagesize = 100;
							FilterService.setFilters(
								"jobs.dataprocessing.pagesize",
								{ pagesize: vm.pagesize }
							);
							vm.currentPage = 1;
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
						label: "Timestamp",
						icon: "numeric",
						property: "timestamp"
					},
					{ label: "Job Status", icon: "alpha", property: "status" }
				]
			}
		};

		angular.extend(vm, {
			jobs: JobsStore.data.importJobs,
			successMsg: null,
			errorMsg: null,
			queuedMsg: null
		});

		vm.rowExpanded = function(row, state) {
			vm.rowStatus[row] = state;
		};
		vm.init = function() {
			vm.jobs = JobsStore.getList("import");

			var filterStore = FilterService.getFilters(
				"jobs.dataprocessing.pagesize"
			);
			if (filterStore) {
				vm.pagesize = filterStore.pagesize;
			}
		};

		this.init();

		function isLastOneFailed() {
			if (vm.jobs && vm.jobs.length > 1) {
				if (vm.jobs[vm.jobs.length - 2].jobStatus === "Failed") {
					return true;
				} else {
					return false;
				}
			} else {
				return false;
			}
		}

		function isOneRunning() {
			// var isOneRunning = false;
			for (let i = 0; i < vm.jobs.length; i++) {
				if (
					vm.jobs[i].jobStatus == "Running" ||
					vm.jobs[i].jobStatus == "Pending"
				) {
					return true;
				}
			}
			return false;
			// vm.jobs.forEach(function (element) {
			//     if (element.jobStatus === 'Running' || element.jobStatus === 'Pending') {
			//         isOneRunning = true;
			//         return isOneRunning;
			//     }
			// });
			// console.log('ONE RUNNING ')
			// return isOneRunning;
		}

		function isOneScheduled() {
			let isOneScheduled = false;
			for (let i = 0; i < vm.jobs.length; i++) {
				if (
					vm.jobs[i].jobStatus === "Running" ||
					vm.jobs[i].jobStatus === "Pending" ||
					(vm.jobs[i].schedulingInfo &&
						vm.jobs[i].schedulingInfo.scheduled == true)
				) {
					isOneScheduled = true;
					return isOneScheduled;
				}
			}
			return isOneScheduled;
		}

		vm.isLastOneFailed = function() {
			return isLastOneFailed();
		};

		vm.canLastJobRun = function() {
			var canRun = false;
			var oneRunnig = isOneRunning();

			if (!oneRunnig) {
				canRun = true;
			}
			return canRun;
		};
		vm.canLastBeScheduled = () => {
			// let canSchedule = false;
			let oneSchedule = isOneScheduled();
			if (!oneSchedule) {
				return true;
			}
			return false;
		};

		vm.clearMessages = function() {
			vm.successMsg = null;
			vm.errorMsg = null;
			vm.queuedMsg = null;
		};
	});
