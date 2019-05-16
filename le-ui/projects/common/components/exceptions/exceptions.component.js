import {BannerService} from './react.messaging.utils';
angular
    .module("common.exceptions", [
        "common.banner",
        "common.modal",
        "common.notice"
    ])
    .service("ServiceErrorUtility", function (
        $timeout,
        $injector,
        Banner,
        Modal,
        Notice
    ) {
        this.check = function (response) {
            //console.log('> check exception', response);
            if (!response || !response.data) {
                return false;
            }

            var data = response.data,
                uiErrorCheck = !!(
                    data.error ||
                    data.error_description ||
                    data.errorMsg
                ),
                uiActionCheck = !!data.UIAction;

            return uiErrorCheck || uiActionCheck;
        };

        this.process = function (response) {
            //console.log('> process exception', response);
            if (this.check(response)) {
                var config = response.config || { headers: {} },
                    params = (
                        config.headers.ErrorDisplayMethod || "banner"
                    ).split("|"),
                    options = config.headers.ErrorDisplayOptions
                        ? JSON.parse(config.headers.ErrorDisplayOptions)
                        : {},
                    callback = config.headers.ErrorDisplayCallback || null,
                    payload = response.data,
                    uiAction = payload ? payload.UIAction : {},
                    method = (uiAction
                        ? uiAction.view
                        : params[0]
                    ).toLowerCase();

                switch (method) {
                    case "none":
                        break;

                    case "popup":
                        this.show(Modal, response, options, callback);
                        break;

                    case "modal":
                        this.show(Modal, response, options, callback);
                        break;

                    case "banner":
                        if(document.getElementById('react-banner-container')){
                            //This method is added from the 
                            //ReactAngularMainComponent once it is mounted
                            this.BannerReact(response, options, callback);
                        }else {
                            this.show(Banner, response, options, callback);
                        }
                        break;

                    case "notice":
                        this.show(Notice, response, options, callback);
                        break;

                    case "suppress":
                        console.log(
                            "-!- API error suppressed:",
                            response,
                            options
                        );
                        break;

                    default:
                        this.show(Modal, response, options, callback);
                }
            }
        };

        this.show = function (Service, response, options, callback) {
            //console.log('> show exception', Service, response, options);
            if (!this.check(response)) {
                return;
            }

            var payload = response.data,
                uiAction = payload.UIAction || {},
                method = (uiAction.status || "error").toLowerCase(),
                http_err = response.statusText,
                http_code = response.status,
                url = response.config
                    ? response.config.url
                    : '',
                title =
                    typeof uiAction.title != "undefined"
                        ? uiAction.title
                        : http_code + ' "' + http_err + '" ' + url,
                message =
                    uiAction.message ||
                    payload.errorMsg ||
                    payload.error_description,
                name = "API_Exception",
                opts = angular.extend(
                    { title: title, message: message, name: name },
                    options
                ),
                cbSplit = callback ? callback.split(".") : [],
                cbService = callback ? $injector.get(cbSplit[0]) : null,
                cbMethod = callback ? cbSplit[1] : null;

            $timeout(function () {
                Service[method](
                    opts,
                    cbMethod ? cbService[cbMethod].bind(cbService) : null
                );
            }, 1);
        };

        this.hideBanner = function () {
            Banner.reset();
        };
    })
    .service("ServiceErrorInterceptor", function ($q, $injector) {
        this.response = function (response) {
            var ServiceErrorUtility = $injector.get("ServiceErrorUtility");
            ServiceErrorUtility.process(response);
            return response || $q.when(response);
        };

        this.request = function (response) {
            var ServiceErrorUtility = $injector.get("ServiceErrorUtility");
            ServiceErrorUtility.process(response);
            return response || $q.when(response);
        };

        this.responseError = function (rejection) {
            var ServiceErrorUtility = $injector.get("ServiceErrorUtility");
            ServiceErrorUtility.process(rejection);
            return $q.reject(rejection);
        };

        this.requestError = function (rejection) {
            var ServiceErrorUtility = $injector.get("ServiceErrorUtility");
            ServiceErrorUtility.process(rejection);
            return $q.reject(rejection);
        };
    })
    .config(function ($httpProvider) {
        $httpProvider.interceptors.push("ServiceErrorInterceptor");
    });
