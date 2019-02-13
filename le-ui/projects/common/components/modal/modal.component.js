angular.module('common.modal', [])
.service('Modal', function($timeout, $compile, $rootScope) {
    var Modal = this;

    this.init = function() {
       this.modals = {};

       this.configTemplate = {
            type: "sm",
            dischargetext: "Cancel",
            confirmtext: "Confirm",
            icon: "fa fa-exclamation-triangle",
            iconstyle: {},
            showclose: true
        };

        this.configs = {
            warning: {
                name: "generic_warning",
                title: "Application Warning",
                confirmcolor: "yellow-button",
                headerconfig: { "background-color":"#ffbd48", "color":"white" },
                showcancel: true
            },
            success: {
                name: "generic_success",
                title: "Success",
                icon: "fa fa-info-circle",
                confirmcolor: "green-button",
                headerconfig: { "background-color":"#6fbe4a", "color":"white" },
                showcancel: false
            },
            error: {
                name: "generic_error",
                title: "Error",
                confirmcolor: "white-button",
                headerconfig: { "background-color":"#D0242F", "color":"white" },
                showcancel: false
            },
            info: {
                name: "generic_info",
                title: "Info",
                icon: "fa fa-info-circle",
                confirmcolor: "blue-button",
                headerconfig: { "background-color":"#629acc", "color":"white" },
                showcancel: false
            },
            generic: {
                name: "generic_modal",
                title: "",
                icon: "",
                confirmcolor: "blue-button",
                headerconfig: { "background-color":"white", "color":"black" },
                showcancel: true
            }
        };
    };

    this.get = function(name) {
        return this.modals[name] ? this.modals[name].modal : false;
    };

    this.getData = function(name){

        if (this.modals[name] && this.modals[name].data !== undefined) {
            return this.modals[name].data ;
        } else {
            return {};
        }
    };

    this.setData = function(name, data){
        var modal = this.modals[name];

        if (modal) {
            this.modals[name].data = data;
        }
    };

    this.set = function(name, modal) {
        var mod = this.modals[name];

        if (!mod) {
            var modalObj = {
                modal: modal
            };

            this.modals[name] = modalObj;
        }
        // this.modals[name] = modal;
    };

    this.remove = function(name) {
        if (this.modals[name]) {
            delete this.modals[name];
        }
    };

    this.getConfig = function(type) {
        var template = Modal.configTemplate;
        var config = Modal.configs[type];
        var combined = angular.extend(template, config);

        return angular.copy(combined);
    };

    this.success = function(opts, cb) {
        this.prefab_generator('success', opts, cb);
    };

    this.warning = function(opts, cb) {
        this.prefab_generator('warning', opts, cb);
    };

    this.error = function(opts, cb) {
        this.prefab_generator('error', opts, cb);
    };

    this.info = function(opts, cb) {
        this.prefab_generator('info', opts, cb);
    };
    this.generic = function(opts, cb) {
        this.prefab_generator('generic', opts, cb);
    };
    this.prefab_generator = function(type, opts, cb) {
        let modal = Modal.get(opts.name);

        if (modal){
            Modal.modalRemoveFromDOM(modal, {name: opts.name});
        }

        var config = Modal.getConfig(type);
        config.callback = cb;
        config = angular.extend(config, opts);
        this.generate(config);
    };

    this.generate = function(config) {
        var scope = $rootScope.$new();

        scope.modalConfig = config;
        scope.modalCallback = this.modalCallback;
        scope.transclusion = config.message || '';

        var directive = '<le-modal-window config="modalConfig" callback-function="modalCallback(args)">'+scope.transclusion+'</le-modal-window>';
        var compiled = $compile(directive)(scope);
        
        if (document.getElementById('leModalContainer')) {
            angular.element('#leModalContainer').append(compiled);
        } else {
            angular.element('body')
                .prepend($('<div></div>')
                    .attr({ id: 'leModalContainer' })
                    .append(compiled));
        }

        var unregisterWatch = $rootScope.$watch(function() {
            return Modal.modals[config.name];
        }, function(newValue, oldValue) {
            if (Modal.modals[config.name]) {
                Modal.get(config.name).toggle();
                unregisterWatch();
            }
        });
    };

    this.modalCallback = function(args) {
        var modal = Modal.get(args.name);

        // if the callback exists and returns FALSE, modal will stay open
        var remove = modal.config.callback
            ? modal.config.callback(args)
            : true;

        if (remove && modal) {
            Modal.modalRemoveFromDOM(modal, args);
        }
    };

    this.modalRemoveFromDOM = function(modal, args) {
        modal.toggle();
        let element = angular.element('#'+args.name);
        if(element){
            element.remove();
        }
        Modal.remove(args.name);
    };

    this.init();
})
.filter('cut', function() {
    return function(value, wordwise, max, tail) {
        if (!value) {
            return '';
        }

        max = parseInt(max, 10);

        if (!max) return value;
        if (value.length <= max) return value;

        value = value.substr(0, max);

        if (wordwise) {
            var lastspace = value.lastIndexOf(' ');

            if (lastspace !== -1) {
                if (value.charAt(lastspace - 1) === '.' || value.charAt(lastspace - 1) === ',') {
                    lastspace = lastspace - 1;
                }

                value = value.substr(0, lastspace);
            }
        }

        return value + (tail || ' …');

    };
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
.directive('leModalWindow', ['Modal', function(Modal) {
    return {
        restrict: 'E',
        scope: { config: '=', callback: '&callbackFunction' },
        replace: true,
        transclude: true,
        templateUrl: "/components/modal/modal.component.html",
        link: function(scope, element, attrs, ctrl, transclude) {
            transclude(scope, function(content) {
                scope.transclude = content.length > 0 ? content[0].innerHTML : '';
            });

            scope.opened = false;
            scope.showModalMsg = false;
            scope.showWaiting = false;
            scope.dischargeDisabled = false;
            scope.confirmDisabled = false;
            scope.modalMsg = '';
            scope.modalMsgType = 'warning';
            scope.iconSupported = {
                "warning": "fa fa-exclamation-triangle"
            };

            if (!scope.config) {
                scope.config = {};
            }

            var name = scope.config['name'] || Date.now();

            Modal.set(name, scope);

            scope.modalConfig = {
                "type": scope.config.type || "md",
                "icon": scope.config.icon || scope.iconSupported.warning,
                "iconstyle": scope.config.iconstyle || {'background-color':'black'},
                "title": scope.config.title || "Default Title",
                "titlelength": scope.config.titlelength || 100,
                "showclose": typeof scope.config.showclose === 'undefined' ? true : scope.config.showclose,
                "showcancel": typeof scope.config.showcancel === 'undefined' ? true : scope.config.showcancel,
                "dischargetext": scope.config.dischargetext || "Cancel",
                "dischargeaction": scope.config.dischargeaction || 'cancel',
                "confirmtext": scope.config.confirmtext || "OK",
                "confirmaction": scope.config.confirmaction || 'ok',
                "confirmstyle" : scope.config.confirmstyle || {},
                "confirmcolor": scope.config.confirmstyle === undefined ? (scope.config.confirmcolor || 'blue-button') : '',
                "contenturl": scope.config.contenturl || '',
                "headerconfig": scope.config.headerconfig || {}
            };

            scope.toggle = function(data) {
                scope.opened = !scope.opened;
                Modal.setData(name, data);
                resetWindow();
            };

            scope.showMessage = function(msg, type) {
                scope.showModalMsg = true;
                scope.modalMsg = msg;
                scope.modalMsgType = type;
            };
            
            scope.waiting = function(show){
                scope.showWaiting = show;

                if (show === true) {
                    scope.disableConfirmButton(true);
                }
            };

            scope.disableConfirmButton = function(disable) {
                this.confirmDisabled = disable;
            };

            scope.disableDischargeButton = function(disable) {
                scope.dischargeDisabled = disable;
            };

            scope.forceDischarge = function(){
                scope.toggle();
                resetWindow();
                scope.callCallback('closedForced');
            };

            scope.discharge = function() {
                scope.callCallback(scope.modalConfig.dischargeaction);
            };

            scope.confirm = function() {
                scope.callCallback(scope.modalConfig.confirmaction);
            };

            scope.callCallback = function(value) {
                if (typeof (scope.callback) !== undefined) {
                    var data = Modal.getData(name);
                    scope.callback({ 
                        args: {
                            name: name,
                            action: value, 
                            data: data 
                        }
                    });
                }
            };

            function resetWindow() {
                scope.showModalMsg = false;
                scope.dischargeDisabled = false;
                scope.confirmDisabled = false;
                scope.showWaiting = false;
                scope.modalMsg = '';
            }
        }
    };
}]);

