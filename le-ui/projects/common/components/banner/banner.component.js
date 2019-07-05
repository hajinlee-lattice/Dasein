angular.module('common.banner', [])
.service('Banner', function($sce) {
    var Banner = this;

    this.init = function() {
        this.banners = [];
    };

    this.get = function(name) {
        if (name) {
            return this.banners.filter(function(banner) {
                return banner.name === name;
            });
        } else {
            return this.banners;
        }
    };

    this.set = function(opts) {
        opts = opts || {};

        var banner = {
            badge: 1,
            name: opts.name || "",
            show: opts.show || true,
            type: opts.type || '',
            title: opts.title || '',
            message: opts.message || ''
        };

        var old = opts.name ? this.get(opts.name) : [];
        
        old = old.filter(function(item) {
            return (item.type == banner.type && item.title == banner.title && item.message.toString() == banner.message.toString());
        });

        if (old.length > 0) {
            old.forEach(function(item) {
                item.timestamp.push(new Date().getTime());
                item.badge++;
            });
        } else {
            banner.timestamp = [ new Date().getTime() ];
            this.banners.push(banner);
        }

        if (Array.isArray(banner.message)) {
            banner.message = handleArrayOfMessages(banner.message);
        } else {
            banner.message = wrapMessage(banner.message);
        }
    };

    function wrapMessage(message) {
        var parts = message.split(' '),
            messageAr = [];

        parts.forEach(function(part) {
            if(part.length > 100) {
                part = '<div class="long-word">' + part + '</div>';
            }
            messageAr.push(part);
        });

        return messageAr.join(' ');
    }

    function handleArrayOfMessages (messages) {
        let messageArr = [];
        messages.forEach(function(message) {
            message = '<li>' + message + '</li>';

            messageArr.push(message);
        });

        return messageArr;
    }

    this.reset = function(lifetime) {
        var now = Date.now();

        lifetime = lifetime || 0;

        var banners = this.banners.filter(function(banner) {
            var ts = banner.timestamp[banner.timestamp.length - 1];
            var age = now - ts;
            
            return age < lifetime;
        });

        this.banners.length = 0;

        banners.forEach(function(banner) {
            Banner.banners.push(banner);
        });
    };

    this.generate = function(type, opts) {
        this.set(angular.extend({
            type: type || 'info'
        }, opts));
    };

    this.error = function(opts) {
        this.generate('error', opts);
    };

    this.warning = function(opts) {
        this.generate('warning', opts);
    };

    this.success = function(opts) {
        this.generate('success', opts);
    };

    this.info = function(opts) {
        this.generate('info', opts);
    };
 
    this.init();
})
.directive('ngHtmlCompile', function($compile) {
    return {
        restrict: 'A',
        link: function(scope, element, attrs) {
            scope.$watch(attrs.ngHtmlCompile, function(newValue, oldValue) {
                element.html(newValue);
                $compile(element.contents())(scope);
            });
        }
    };
})
.component('bannerMessage', {
    templateUrl: '/components/banner/banner.component.html',
    controller: function(Banner, $timeout) {
        var vm = this;

        vm.$onInit = function() {
            vm.banners = Banner.get();
        };

        vm.isVisible = function() {
            var visible = false;

            vm.banners.forEach(function(banner, index) {
                if (banner.show === true) {
                    visible = true;
                }
            });

            return visible;
        };

        vm.showTimeStamps = function(banner) {
            var now = Date.now();
            var times = [];

            banner.timestamp.forEach(function(ts, index) {
                var elapsed = new Date(now - ts),
                    index = index + 1,
                    secs = elapsed.getSeconds(),
                    mins = elapsed.getMinutes(),
                    time = '    Iteration ' + index + ':    now',
                    fn = function(num, text) {
                        return (num == 1 ? text : text + 's') + ' ';
                    };

                if (mins > 0) {
                    time = '    Iteration ' + index + ':    ' + mins + fn(mins,' minute') + secs + fn(secs,' second') + 'ago';
                } else if (secs > 0) {
                    time = '    Iteration ' + index + ':    ' + secs + fn(secs,' second') + 'ago';
                }

                times.push(time);
            });

            var preface = 'This banner' + (banner.name ? ' "' + banner.name + '" ' : ' ') + 'has been triggered ' + banner.badge + ' times.\n\n'

            return preface + times.join('\n');
        };

        vm.resizeBanner = function() {
            var $_banner_message = angular.element('banner-message .messaging-message'),
                initialWidth = $_banner_message.parent().innerWidth() - 20;

            $timeout(function() {
                var $banner_message = angular.element('banner-message .messaging-message'),
                    hasLongWord = $banner_message.find('.long-word').length;

                if(hasLongWord) {
                    $banner_message.width(initialWidth);
                }
            });
        }
    }
});