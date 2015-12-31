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
        angular.extend($scope, AttributesModel);
    }
);