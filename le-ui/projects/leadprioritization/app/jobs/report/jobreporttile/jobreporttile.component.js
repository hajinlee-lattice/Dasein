angular
.module('lp.jobs')
.directive('jobsSummaryTile', function(ModelStore) {
    return {
        restrict: 'EA',
        templateUrl: 'app/jobs/report/jobreporttile/jobreporttile.component.html',
        scope: {
        	report: '=',
        	entity: '=',
        	count: '=',
        	actions: '='
        },
        controllerAs: 'vm',
        controller: function($scope, JobsStore, JobsService, CancelJobModal, BrowserStorageUtility) {
        	var vm = this;

        	angular.extend(vm, {
        		report: $scope.report,
        		entity: $scope.entity,
        		count: $scope.count,
        		actions: $scope.actions,
        		filteredActions: [],
        		total: 0,
        		newRecords: 0,
        		updatedRecords: 0,
        		matchedRecords: 0,
        		deletedRecords: 0,
        		unmatchedRecords: 0
        	});
        	
        	vm.init = function() {
        		if (vm.report) {
        			vm.newRecords = vm.report['NEW'] ? vm.report['NEW'] : 0;
        			vm.updatedRecords = vm.report['UPDATE'] ? vm.report['UPDATE']: 0;
        			vm.matchedRecords = vm.report['MATCH'] ? vm.report['MATCH'] : 0;
        			vm.deletedRecords = 0;
        			vm.unmatchedRecords = vm.getUnmatchedRecords();
        		}
        		vm.setFilteredActions();
        	}

        	vm.getUnmatchedRecords = function() {
        		if (vm.report) {
        			return vm.newRecords - vm.matchedRecords;
        		}
        	}

        	vm.format = function(entity) {
        		if (entity != 'Transaction') {
        			return entity.replace(/_/g, " ") + 's';
        		} else {
        			return 'Product Purchases'
        		}
        	}

        	vm.entityIcon = function(entity) {
                var path = '/assets/images/';
                switch (vm.entity) {
                	case 'Account':
                		return path + 'ico-accounts-dark.png';
                	case 'Contact': 
                		return path + 'ico-contacts-dark.png';
                	case 'Product':
                		return path + 'ico-products.png';
                	case 'Transaction':
                		return path + 'ico-transactions.png';
                	default:
                		return path + 'enrichments/subcategories/contactattributes.png'
	            }

                return path + icon;
            }

			vm.setFilteredActions = function() {
				vm.filteredActions = [];
				vm.actions.forEach(function(action) {
					if (vm.parsePayload(action, vm.entity) != undefined) {
						vm.filteredActions.push(action)
					}
				});
			}

			vm.parsePayload = function(action, key) {
				return JSON.parse(action.reports[0]['json']['Payload'])[key];
			}

            vm.showUpdateTile = function() {
                return vm.entity != 'Transaction';
            }

            vm.showUnmatchTile = function() {
                return vm.unmatchedRecords && vm.entity == 'Account';
            }
        	

        	vm.init();


        }
    };
});