angular.module('pd.navigation.subnav', [])
    .controller('SubNavSummaryCtrl', function($scope, $rootScope) {
        $scope.CompaniesTotal = 0;
        $scope.RevenueTotal = 0;

        $scope.$on('Builder-Sidebar-List', function(event, args) {
            $scope.CompaniesTotal = 0;
            $scope.RevenueTotal = 0;
            
            args.forEach(function(item, key) {
                $scope.CompaniesTotal += parseInt(item.Properties.CompanyCount);
                $scope.RevenueTotal += parseInt(item.revenue);
            });

        });
    })
    .controller('SubNavCtrl', function ($scope, $rootScope) {
        this.init = function() { 
            this.lis = lis = $('div.carousel-slide-container div.white-border span');
            $(lis).on('mousedown', this.handleClick.bind(this));
        }

        this.getMouseXY = function(target, event) {
            return {
                x: event.offsetX,
                y: event.offsetY
            }
        }

        this.clamp = function(number, min, max) {
            return Math.max(min, Math.min(number, max));
        }

        this.handleClick = function(event) {
            var target = event.target;//,
                //target = target.tagName == 'LI' ? target : target.parentNode;

            this.render(target, event);
        }

        this.render = function(li, event) {
            var lis = this.lis,
                item = li,//$('div.carousel-slide-container a>span',li)[0],
                mouse = this.getMouseXY(li, event),
                dimensions = item.getClientRects()[0],
                center = {
                    x: dimensions.width >> 1,
                    y: dimensions.height >> 1
                },
                delta = {
                    x: (mouse.x - center.x) / center.x,
                    y: (center.y - mouse.y) / center.y
                }, 
                origin = {
                    x: this.clamp(1 - ((dimensions.width - mouse.x) / dimensions.width), 0, 1),
                    y: this.clamp(1 - ((dimensions.height - mouse.y) / dimensions.height), 0, 1)
                },
                flipThreshold = .70,
                maxRotation = 4,
                maxAngle = 33,
                minDuration = 500,
                maxDuration = 1250,
                magnitude = Math.min(Math.max(Math.abs(delta.x), Math.abs(delta.y)), 1),
                x = Math.min(Math.round(delta.x * maxRotation), maxRotation) * 360,
                y = Math.min(Math.round(delta.y * maxRotation), maxRotation) * 360,
                angle = Math.atan2(y, x) * (180 / Math.PI),
                duration = ((1 - magnitude) * (maxDuration - minDuration)) + minDuration,
                transition, transform, perspective, transitionEvent;

            if (magnitude > flipThreshold) {
                transform = 'rotate3d(' + delta.y + ', ' + delta.x + ', 0, 360deg) translate3d(0,0,0px)';
                perspective = Math.round(origin.x * 100) + '% ' + Math.round(origin.y * 100) + '%';
            } else {
                duration = duration >> 2.5;
                percentage = magnitude / flipThreshold;
                angle = percentage * maxAngle;
                transform = 'rotate3d(' + delta.y + ', ' + delta.x + ', 0, ' + angle + 'deg) translate3d(0,0,-8px)';
                perspective = Math.round(origin.x * 100) + '% ' + Math.round(origin.y * 100) + '%';
            }

            transition = 'transform ' + duration + 'ms ease';
            transitionEvent = this.whichTransitionEvent();
            console.log('perspective:',perspective,', magnitude:',magnitude, ', angle:',angle, ', duration:',duration, '\n', transition, transform);
            item.style.transition = 'transform 0ms linear';
            //item.style.transform = 'rotate3d(0, 0, 0, 0deg) translate3d(0,0,0px)'; 
            li.style.perspectiveOrigin = '50% 50%';
            
            setTimeout(function() {
                item.style.transition = 'transform '+duration+'ms ease';
                item.style.transform = transform;
                li.style.perspectiveOrigin = perspective;

                //console.log(origin.x, origin.y, duration, delta.x, delta.y,  transitionEvent, transform, perspective);
                
                $(item).one(transitionEvent, function(event) {
                    if (magnitude > flipThreshold) {
                        item.style.transition = 'transform 0s linear';
                        item.style.transform = 'rotate3d(0, 0, 0, 0deg) translate3d(0,0,0px)'; 
                        li.style.perspectiveOrigin = '50% 50%';
                    } else {
                        item.style.transition = 'transform ' + duration << 3 + ' linear';
                        item.style.transform = 'rotate3d(0, 0, 0, 0deg) translate3d(0,0,0px)'; 
                        li.style.perspectiveOrigin = '50% 50%';
                    }
                    
                    $(lis).removeClass('active');
                    $(li).addClass('active');
                });
            }, 0);
        }

        this.whichTransitionEvent = function() {
          var t,
              el = document.createElement("fakeelement");

          var transitions = {
            "transition"      : "transitionend",
            "OTransition"     : "oTransitionEnd",
            "MozTransition"   : "transitionend",
            "WebkitTransition": "webkitTransitionEnd"
          }

          for (t in transitions){
            if (el.style[t] !== undefined){
              return transitions[t];
            }
          }
        }


        this.init();
    }
);