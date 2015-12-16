angular.module('pd.builder', [

])

.service('BuilderService', function($http, $q, _) {
    this.GetCategories = function(key) {
        var deferred = $q.defer();
        var result;
        
        $http({
            method: 'GET',
            url: '/pls/amattributes?AttrKey='+key
        }).then(
            function onSuccess(response) {
                deferred.resolve(response);
            }, function onError(response) {

            }
        );
        
        return deferred.promise;
    };
})
.controller('BuilderCtrl', function($scope, $rootScope, $stateParams, BuilderService) {
    $scope.lists = [];
    $scope.AttrKey = AttrKey = $stateParams.AttrKey;

    if (!AttrKey) {
        return console.log('<!> No stateParams provided.');
    }

    // will fetch/format all attributes for given category
    BuilderService.GetCategories(AttrKey).then(function(result) {
        var data = result.data || [],
            chunks = 3,
            truncate_limit = 24, // truncate with ellipsis
            total = $scope.total = data.length;

        // FIXME - Fudging the numbers a bit for the demo...
        data.forEach(function(item, index) {
            item.total = Math.round(Math.random() * 20);
            item.lift = (Math.random() * 3.0).toFixed(1) + 'x';

            item.revenue = Math.round(Math.random() * 30000000);
            
            var nums = [
                Math.round(Math.random() * 99999),
                Math.round(Math.random() * 99999),
                Math.round(Math.random() * 9999)
            ];

            nums.sort();
            item.lattice = nums[0];
            item.database = nums[1];
            item.customers = nums[2];

            item.database = item.database > item.lattice ? item.lattice >> 1 : item.database;
            item.customers = item.customers > item.database ? item.database >> 1 : item.customers

            item.mediump = ((item.database / item.lattice) * 100).toFixed(3);
            item.smallp = ((item.customers / item.lattice) * 100).toFixed(3);

            item.mediump = item.mediump < 16 ? 16 : item.mediump;
            item.smallp = item.smallp < 10 ? 10 : item.smallp;
        });

        // Break the array up into rows of 3 each
        data = _.chain(data).groupBy(function(element, index) {
            return Math.floor(index / chunks);
        }).toArray().value();

        $scope.lists = data;
        $scope.truncate_limit = truncate_limit;

        console.log('builder category list:', $scope.lists);
    });
});