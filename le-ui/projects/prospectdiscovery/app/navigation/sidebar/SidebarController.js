angular
    .module('pd.navigation.sidebar', [
    ])
    .controller('SidebarCtrl', function($scope, $rootScope) {
        $scope.toggle = function() {
            $("body").toggleClass("open-nav");
        }
    })
    .controller('BuilderSidebarCtrl', function($scope, $rootScope) {
        $scope.Industry = [];
        $scope.State = [];
        $scope.EmployeesRange = [];
        $scope.TotalAttributes = 0;
        
        $scope.$on('Builder-Sidebar-List', function(event, args) {
            $scope.Industry = [];
            $scope.State = [];
            $scope.EmployeesRange = [];
            $scope.TotalAttributes = args.length;

            args.forEach(function(item, key) {
                $scope[item.AttrKey].push(item);
            });
        });
    }
);