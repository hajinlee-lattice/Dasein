angular.module('lp.ratingsengine.remodel.attributes', [])
.config(function($stateProvider) {
    $stateProvider
        .state('home.ratingsengine.remodel.training.attributes', {
            url: '/attributes',
            onExit: ['AtlasRemodelStore', function(AtlasRemodelStore) {
                AtlasRemodelStore.init();
            }],
            resolve: {
                attributes: ['$q', '$stateParams', 'AtlasRemodelStore', function ($q, $stateParams, AtlasRemodelStore) {

                    var deferred = $q.defer(),
                        engineId = $stateParams.engineId,
                        modelId = $stateParams.modelId;

                    AtlasRemodelStore.getAttributes(engineId, modelId).then(function(attributes) {
                        deferred.resolve(attributes);
                    });

                    return deferred.promise;

                }]
            },
            views: {
                'wizard_content@home.ratingsengine.remodel': 'remodelAttributes'
            }
        });
})
.component('remodelAttributes', {
    templateUrl: 'app/ratingsengine/content/remodel/attributes/attributes.component.html',
    bindings: {
        attributes: '<'
    },
    controller: function (
        $q, $scope, $stateParams, $timeout,
        AtlasRemodelStore
    ) {

        var vm = this;

        vm.$onInit = function() {

            vm.store = AtlasRemodelStore;

            // Set attributes data
            vm.store.setRemodelAttributes(vm.attributes);

            // Create categories object to render categories tabs
            vm.categories = {};
            // Get categories and length of items
            angular.forEach(vm.attributes, function(value, key){
                vm.categories[key] = value.length;
            });

            vm.filters = vm.store.getFilters();

            // Set default category in the store
            vm.selectedCategory = Object.keys(vm.categories)[0];
            vm.setCategoryData(vm.selectedCategory);

            vm.filters = AtlasRemodelStore.getFilters();

        }

        vm.setCategoryData = function(category){
            AtlasRemodelStore.set('category', category);
        }
    }
});