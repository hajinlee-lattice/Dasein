angular
    .module('pd.navigation.sidebar', [
        'pd.builder.attributes'
    ])
    .controller('SidebarCtrl', function($scope, $rootScope) {
        $scope.toggle = function() {
            $("body").toggleClass("open-nav");
        }
    })
    .controller('BuilderSidebarCtrl', function($scope, AttributesModel) {
        console.log('builder sidebar controller:', $scope);
        angular.extend($scope, AttributesModel);
        /*
        $scope.$watch('MasterList', function (lists) {
            $scope.flat_attributes = [];
            
            Object.keys(lists).forEach(function(key, index) {
                lists[key].items.forEach(function(item, index) {
                    if (item.selected)
                        $scope.flat_attributes.push(item);
                });
            });
        }, true);
        */
    }
);