angular
.module('common.datacloud.query.advanced.tree', [])
.directive('queryTreeDirective',function() {
    return {
        restrict: 'AE',
        scope: {
            root: '=',
            tree: '=',
            parent: '='
        },
        templateUrl: '/components/datacloud/query/advanced/tree/tree.component.html',
        controllerAs: 'vm',
        controller: function ($scope, DataCloudStore, QueryStore) {
            var vm = this;

            angular.extend(vm, {
                root: $scope.root,
                tree: $scope.tree,
                parent: $scope.parent,
                items: $scope.root.items,
                enrichments: [],
                enrichmentsMap: DataCloudStore.getEnrichmentsMap()
            });

            vm.init = function (type, value) {
                vm.tree.collapsed = false;

                DataCloudStore.getEnrichments().then(function(enrichments) {
                    vm.enrichments = enrichments;
                    
                    if (vm.tree.bucketRestriction) {
                        vm.item = vm.enrichments[ vm.enrichmentsMap[ vm.tree.bucketRestriction.attr.split('.')[1] ] ]
                    }
                });
            }

            vm.addAttribute = function(tree) {
                this.root.saveState();
                QueryStore.setAddBucketTreeRoot(vm.tree);
                this.root.goAttributes();
            }

            vm.addOperator = function(tree) {
                var operator = tree.logicalRestriction.operator == 'AND' ? 'OR' : 'AND';
                this.root.saveState();

                if (tree.logicalRestriction) {
                    tree.logicalRestriction.restrictions.push({
                        logicalRestriction: {
                            operator: operator,
                            restrictions: []
                        }
                    })
                }
            }

            this.clickOperator = function(value) {
                this.root.saveState();
                vm.tree.logicalRestriction.operator = value;
            }

            this.clickCollapsed = function() {
                this.root.saveState(true);
                vm.tree.collapsed = !vm.tree.collapsed;
            }

            this.clickDelete = function() {
                this.root.saveState();

                vm.parent.logicalRestriction.restrictions.forEach(function(item, index) {
                    if (item == vm.tree) {
                        if (vm.parent.bucketRestriction || vm.parent.logicalRestriction) {
                            vm.parent.logicalRestriction.restrictions.splice(index, 1);
                        }
                    }
                });
            }

            vm.init();
        }
    };
});