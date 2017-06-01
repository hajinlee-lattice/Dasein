angular.module('lp.cg.talkingpoint.attributepane', [
])
.directive('cgTpAttributePane', function() {
    return {
        restrict: 'E',
        replace: true,
        scope: {
            opts: '='
        },
        templateUrl: 'app/cgtalkingpoint/attributepane/attributepane.component.html',
        controller: function($scope, $element, $window) {
            $scope.selected = Object.keys($scope.opts)[0];

            $window.addEventListener('scroll', handleWindowScroll);

            $scope.$on('$destroy', function() {
                $window.removeEventListener('scroll', handleWindowScroll);
            });

            var originalTop = $element[0].offsetTop;
            function handleWindowScroll(evt) {
                var scrollY = evt.currentTarget.scrollY;
                if (scrollY > originalTop) {
                    $element[0].style.top = (scrollY - originalTop + 78) + 'px';
                } else {
                    $element[0].style.top = '';
                }
            }

            $scope.eleMouseDown = function($event) {
                $event.preventDefault();
                $event.stopPropagation();

                $event.target.style.width = '200%';
                $event.target.style.left = '-200%';

                $window.addEventListener('mousemove', eleMouseMove, false);
                $window.addEventListener('mouseup', eleMouseUp, false);
            };

            var originalX = null, originalWidth;
            function eleMouseMove(evt) {
                evt.preventDefault();
                evt.stopPropagation();

                if (originalX === null) {
                    originalX = evt.clientX;
                    originalWidth = $element[0].offsetWidth;
                }

                var dx = originalX - evt.clientX;
                $element[0].style.width = originalWidth + dx + 'px';
            };

            function eleMouseUp(evt) {
                evt.preventDefault();
                evt.stopPropagation();

                evt.target.style.width = '';
                evt.target.style.left = '';

                originalX = null;
                originalWidth = null;

                $window.removeEventListener('mousemove', eleMouseMove, false);
                $window.removeEventListener('mouseup', eleMouseUp, false);
            };
        },
        controllerAs: 'vm'
    }
})
.directive('attributeDraggable', function() {
    return {
        restrict: 'E',
        replace: true,
        scope: {
            attribute: '='
        },
        templateUrl: 'app/cgtalkingpoint/attributepane/attributedraggable.component.html',
        controller: function($scope, $element) {
            $element[0].addEventListener('dragstart', onDragStart);

            $scope.$on('$destroy', function() {
                $element[0].removeEventListener('dragstart', onDragStart);
            });

            function onDragStart(evt) {
                evt.dataTransfer.dropEffect = 'move';
                evt.dataTransfer.effectAllowed = 'all';
                evt.dataTransfer.setData('text', '{!' + $scope.attribute.value + '}');
            }
        }
    }
});
