angular
    .module('common.datacloud.query.builder.input', [])
    .directive('queryInputDirective', function () {
        return {
            restrict: 'AE',
            scope: {
                root: '=',
                tree: '='
            },
            templateUrl: '/components/datacloud/query/advanced/input/input.component.html',
            controllerAs: 'vm',
            controller: function ($scope, DataCloudStore) {
                var vm = this;

                angular.extend(vm, {
                    root: $scope.root,
                    tree: $scope.tree,
                    items: $scope.root.items,
                    enrichments: [],
                    enrichmentsMap: DataCloudStore.getEnrichmentsMap()
                });

                vm.init = function (type, value) {
                    DataCloudStore.getEnrichments().then(function (enrichments) {
                        vm.enrichments = enrichments;

                        if (vm.tree.bucketRestriction) {
                            vm.item = vm.enrichments[vm.enrichmentsMap[vm.tree.bucketRestriction.attr]];
                        }
                    });
                }

                vm.categoryClassInput = function (category = '') {
                    // console.log('[input]', category);
                    var cat = category.toLowerCase().replace(/\s/g, "-");
                    return cat;
                }

                vm.init();
            }
        };
    });