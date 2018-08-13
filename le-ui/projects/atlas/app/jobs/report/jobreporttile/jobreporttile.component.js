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
        		unmatchedRecords: 0,
                hasErrorMessage: false,
                showCuratedAttributesTable: $scope.entity == 'PurchaseHistory',
                showActionsTable: $scope.entity != 'PurchaseHistory'
        	});
        	
        	vm.init = function() {
        		if (vm.report) {
        			vm.newRecords = vm.report['NEW'] ? vm.report['NEW'] : 0;
        			vm.updatedRecords = vm.report['UPDATE'] ? vm.report['UPDATE']: 0;
        			vm.deletedRecords = vm.report['DELETE'] ? vm.report['DELETE']: 0;
        			vm.unmatchedRecords = vm.report['UNMATCH'] ? vm.report['UNMATCH'] : 0;
                    vm.updatedProductHierarchy = vm.report['PRODUCT_HIERARCHY'] ? vm.report['PRODUCT_HIERARCHY'] : 0;
                    vm.updatedProductBundle = vm.report['PRODUCT_BUNDLE'] ? vm.report['PRODUCT_BUNDLE'] : 0;
                    vm.hasErrorMessage = vm.report['ERROR_MESSAGE'] != undefined ? vm.report['ERROR_MESSAGE'] : false; 
                    if (vm.entity == 'PurchaseHistory') {
                        vm.curatedAttributes = vm.report['ACTIONS'] ? vm.report['ACTIONS'] : {};
                    }
        		}
                if (vm.showActionsTable) {
                    vm.setFilteredActions();
                }
                vm.config = vm.getConfig();
        	}

        	vm.format = function(entity) {
        		if (entity != 'Transaction' && entity != 'PurchaseHistory') {
        			return entity.replace(/_/g, " ") + 's';
        		} else if (entity == 'Transaction'){
        			return 'Product Purchases';
        		} else if (entity == 'PurchaseHistory') {
                    return 'Curated Attributes';
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
                		return path + 'ico-product_hierarchy-dark.png';
                	case 'Transaction':
                		return path + 'ico-purchases-dark.png';
                	default:
                		return path + 'enrichments/subcategories/contactattributes.png';
	            }

                return path + icon;
            }

			vm.setFilteredActions = function() {
				vm.filteredActions = [];
				vm.actions.forEach(function(action) {
					if (action.reports && action.reports.length > 0 && vm.parsePayload(action, vm.entity) != undefined) {
						vm.filteredActions.push(action)
					}
				});
			}

			vm.parsePayload = function(action, key) {
				return JSON.parse(action.reports[0]['json']['Payload'])[key] || JSON.parse(action.reports[0]['json']['Payload'])[key + '_Deleted'];
			}

            vm.hasErrorMessage = function() {
                return vm.entity == 'Product' && vm.report['ERROR_MESSAGE'];
            }

            vm.getConfig = function() {
                switch (vm.entity) {
                    case 'Account': 
                        return {
                                'new': true,
                                'updated': true,
                                'deleted': true,
                                'unmatched': vm.unmatchedRecords > 0
                                };
                    case 'Contact': 
                        return {
                                'new': true,
                                'updated': true,
                                'deleted': true
                                };
                    case 'Product': 
                        return {
                                'product_hierarchy': true,
                                'product_bundle': true
                                };
                    case 'Transaction': 
                        return {
                                'new': true,
                                'deleted': true
                                };
                    case 'PurchaseHistory': 
                        return {};
                }
            }
        	

        	vm.init();


        }
    };
});