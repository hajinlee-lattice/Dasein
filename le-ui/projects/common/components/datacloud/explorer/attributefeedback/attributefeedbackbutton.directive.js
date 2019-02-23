export default function () {
    return {
        restrict: 'EA',
        templateUrl: '/components/datacloud/explorer/attributefeedback/attributefeedbackbutton.component.html',
        scope: {
            buttonType: '=?',
            attribute: '=',
            attributeKey: '=?',
            value: '=',
            iconImage: '=?',
            iconFont: '=?',
            categoryClass: '=?',
            label: '=?',
            showLookupStore: '=?',
            type: '=?'
        },
        controller: function ($scope, DataCloudStore) {
            'ngInject';

            $scope.buttonType = $scope.buttonType || 'infodot';

            $scope.menuClick = function ($event) {
                $event.stopPropagation();
                $scope.showMenu = !$scope.showMenu;
            }

            $scope.closeMenu = function ($event) {
                $scope.showMenu = false;
            }

            $scope.open = function ($event) {
                $event.stopPropagation();
                $scope.closeMenu($event);
                DataCloudStore.setFeedbackModal(true, {
                    attribute: $scope.attribute,
                    attributeKey: $scope.attributeKey,
                    value: $scope.value,
                    icon: {
                        image: $scope.iconImage || '',
                        font: $scope.iconFont || ''
                    },
                    categoryClass: $scope.categoryClass,
                    label: $scope.label || '',
                    showLookupStore: $scope.showLookupStore,
                    type: $scope.type
                });
            }
        }
    };
};