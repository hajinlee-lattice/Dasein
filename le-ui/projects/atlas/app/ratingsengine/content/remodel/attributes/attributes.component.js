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

                }],
                configfilters: ['$q', 'RatingsEngineStore', function($q, RatingsEngineStore){
                    var deferred = $q.defer();
                    var copy = angular.copy(RatingsEngineStore.getConfigFilters());
                    deferred.resolve(copy);
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
        attributes: '<',
        filters: '<',
        configfilters: '<'
    },
    controller: function (
        $q, $scope, $stateParams, $timeout,
        AtlasRemodelStore
    ) {

        var vm = this;

        vm.$onInit = function() {
            vm.store = AtlasRemodelStore;
            vm.store.setConfigFilters(vm.configfilters);
            // Move Lead Information attributes to My Attributes and delete Lead Information Category
            if(vm.attributes['Lead Information']){
                angular.forEach(vm.attributes['Lead Information'], function(attribute){
                    attribute.Category = 'My Attributes';
                    vm.attributes['My Attributes'].push(attribute);
                });
                delete vm.attributes['Lead Information'];
            }

            // Set attributes data
            vm.store.setRemodelAttributes(vm.attributes);

            // Create categories object to render categories tabs
            vm.categories = {};
            angular.forEach(vm.attributes, function(value, key){
                vm.categories[key] = value.length;
            });

            vm.filters = vm.store.getFilters();

            // Set default category in the store
            vm.selectedCategory = Object.keys(vm.categories)[0];
            vm.setCategoryData(vm.selectedCategory);
        }

        vm.setCategoryData = function(category){
            vm.filters.currentPage = 1;
            console.log(category, vm.filters);
            AtlasRemodelStore.set('category', category);
        }
    }
});