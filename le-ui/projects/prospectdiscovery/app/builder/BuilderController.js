angular.module('pd.builder', [

])

.service('BuilderService', function($http, $q, _) {
    this.GetCategories = function(key) {
        var deferred = $q.defer();
        var result;
        
        $http({
            method: 'GET',
            url: '/pls/amattributes?Key='+key
        }).then(
            function onSuccess(response) {
                var data = [
                    {category: key + " Category",lift: "0.3x",revenue: "$9,324,532",lattice: "2,393,223",database: "154,232",customers: "123"},
                    {category: key + " Category",lift: "1.9x",revenue: "$9,324,532",lattice: "2,393,223",database: "154,232",customers: "123"},
                    {category: key + " Category",lift: "0.9x",revenue: "$9,324,532",lattice: "2,393,223",database: "154,232",customers: "123"},
                    {category: key + " Category",lift: "0.4x",revenue: "$9,324,532",lattice: "2,393,223",database: "154,232",customers: "123"},
                    {category: key + " Category",lift: "1.9x",revenue: "$9,324,532",lattice: "2,393,223",database: "154,232",customers: "123"},
                    {category: key + " Category",lift: "0.9x",revenue: "$9,324,532",lattice: "2,393,223",database: "154,232",customers: "123"},
                    {category: key + " Category",lift: "2.5x",revenue: "$9,324,532",lattice: "2,393,223",database: "154,232",customers: "123"},
                    {category: key + " Category",lift: "0.9x",revenue: "$9,324,532",lattice: "2,393,223",database: "154,232",customers: "123"},
                    {category: key + " Category",lift: "1.9x",revenue: "$9,324,532",lattice: "2,393,223",database: "154,232",customers: "123"},
                    {category: key + " Category",lift: "0.2x",revenue: "$9,324,532",lattice: "2,393,223",database: "154,232",customers: "123"},
                    {category: key + " Category",lift: "1.3x",revenue: "$9,324,532",lattice: "2,393,223",database: "154,232",customers: "123"},
                    {category: key + " Category",lift: "0.9x",revenue: "$9,324,532",lattice: "2,393,223",database: "154,232",customers: "123"},
                    {category: key + " Category",lift: "0.5x",revenue: "$9,324,532",lattice: "2,393,223",database: "154,232",customers: "123"},
                    {category: key + " Category",lift: "1.7x",revenue: "$9,324,532",lattice: "2,393,223",database: "154,232",customers: "123"},
                    {category: key + " Category",lift: "0.9x",revenue: "$9,324,532",lattice: "2,393,223",database: "154,232",customers: "123"},
                    {category: key + " Category",lift: "0.8x",revenue: "$9,324,532",lattice: "2,393,223",database: "154,232",customers: "123"}
                ];

                var chunks = 4;
                
                response.data = _.chain(data).groupBy(function(element, index) {
                  return Math.floor(index / chunks);
                }).toArray().value();

                deferred.resolve(response);
            }, function onError(response) {

            }
        );
        
        return deferred.promise;
    };
})
.controller('BuilderCtrl', function($scope, $rootScope, BuilderService) {
    $scope.lists = [];

    BuilderService.GetCategories('Industry').then(function(result) {
        $scope.lists = result.data;
        console.log($scope.lists);
    });
});