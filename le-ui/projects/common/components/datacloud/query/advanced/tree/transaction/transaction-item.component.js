angular
    .module('common.datacloud.query.builder.tree.edit.transaction', [])
    .directive('transactionDirective', function () {
        return {
            restrict: 'E',
            scope: {
                type: '=',
                bucketrestriction: '='
            },
            templateUrl: '/components/datacloud/query/advanced/tree/transaction/transaction-item.component.html',
            controllerAs: 'vm',
            controller: function ($scope, $timeout, $state, DataCloudStore, QueryStore, QueryTreeService) {
                var vm = $scope.vm;
                vm.cmpsList = [
                    { 'name': 'EVER', 'displayName': 'Ever' },
                    { 'name': 'IN_CURRENT_PERIOD', 'displayName': 'In' },
                    { 'name': 'BETWEEN', 'displayName': 'Between' },
                    { 'name': 'PRIOR_ONLY', 'displayName': 'Prior' },
                    { 'name': 'WITHIN', 'displayName': 'Within' },
                ];

                vm.cmp = QueryTreeService.getCmp($scope.bucketrestriction, $scope.type);
                console.log(vm.cmp);

                //************************ Txn *********************/
                vm.getCmpsList = function () {
                    var cmpsList = [
                        'EVER',
                        'IN_CURRENT_PERIOD',
                        'BETWEEN',
                        'PRIOR_ONLY',
                        'WITHIN'
                    ];
                    return cmpsList;
                }

                vm.getFromId = function () {
                    var id = $scope.bucketrestriction.attr;
                    return id + '.txn_from';
                }

                vm.getToId = function () {
                    var id = $scope.bucketrestriction.attr;
                    return id + '.txn_to';
                }

                vm.changeCmp = function () {

                }

                vm.showFrom = function () {
                    if (vm.cmp == 'BETWEEN') {
                        return true;
                    } else {
                        return false;
                    }
                }
                vm.showTo = function () {
                    if (vm.cmp == 'BETWEEN' || vm.cmp == 'PRIOR_ONLY') {
                        return true;
                    } else {
                        return false;
                    }
                }

                vm.changeCmp = function () {
                    $timeout(initDatePicker, 0);
                }

                function initDatePicker() {
                    var from = document.getElementById(vm.getFromId());

                    if (from != null) {
                        console.log('Found the picker');
                        var fromPicker = new Pikaday({ field: from });
                    }
                    var to = document.getElementById(vm.getToId());

                    if (to != null) {
                        console.log('Found the picker');
                        var toPicker = new Pikaday({ field: to });
                    }
                }
                $timeout(initDatePicker, 0);

                /*********************** Qty *************************/
                vm.unitPurchasedCmpChoises = [
                    { 'name': 'EQUAL', 'displayName': 'Equal' },
                    { 'name': 'NOT_EQUAL', 'displayName': 'Not Equal' },
                    { 'name': 'GREATER_THAN', 'displayName': 'Greater Than' },
                    { 'name': 'GREATER_OR_EQUAL', 'displayName': 'Greater or Equal' },
                    { 'name': 'LESS_THAN', 'displayName': 'Less Than' },
                    { 'name': 'LESS_OR_EQUAL', 'displayName': 'Lesser or Equal' },
                    { 'name': 'GTE_AND_LT', 'displayName': 'Between' },
                    { 'name': 'ANY', 'displayName': 'Any' },
                ];
                // QueryTreeService.numerical_operations;
                // vm.unitPurchasedCmpChoises['ANY'] = 'Any';
                var tmp = QueryTreeService.getCmp($scope.bucketrestriction, $scope.type, 'Qty');
                vm.unitPurchasedCmp = tmp !== '' ? tmp : 'ANY';
                

                vm.showFromUnit = function () {
                    switch (vm.unitPurchasedCmp) {
                        case 'EQUAL':
                        case 'GREATER_OR_EQUAL':
                        case 'GREATER_THAN':
                        case 'GTE_AND_LT':
                        case 'NOT_EQUAL':
                            {
                                return true;
                            }
                        default: return false;
                    }
                }
                vm.showToUnit = function () {
                    switch (vm.unitPurchasedCmp) {
                        case 'GTE_AND_LT':
                        case 'LESS_OR_EQUAL': 
                        case 'LESS_THAN':{
                            return true;
                        }
                        default: return false;
                    }
                }


                vm.changeUnitPurchasedCmp = function () {
                   
                }



                /************************ Amt *******************************/



            }
        }
    });