//Initial load of the application
import httpService from "../../common/app/http/http-service";
import messagingService from "../../common/app/utilities/messaging-service";
import {
  MODAL,
  BANNER,
  NOTIFICATION,
  ERROR,
  INFO,
  WARNING
} from "../../common/app/utilities/message";
var mainApp = angular
  .module("mainApp", [
    "templates-main",
    //'ngAnimate',
    "ngRoute",
    "ui.router",
    "ui.bootstrap",
    "oc.lazyLoad",
    "angulartics",
    "angulartics.mixpanel",
    "mainApp.appCommon.Widgets",

    "common.modules",
    "common.modal",
    "common.banner",
    "common.notice",
    "common.exceptions",
    "common.attributes",
    "common.datacloud",

    //'lp.header',
    "pd.navigation",
    "lp.jobs",
    // "lp.campaigns",
    // "lp.campaigns.models",
    // "lp.segments.segments",
    "lp.models.list",
    "lp.models.review",
    "lp.models.ratings",
    "lp.notes",
    // "lp.playbook",
    // "lp.ratingsengine",
    "lp.importtemplates",
    "lp.import",
    "lp.delete",
    "lp.create.import",
    "lp.sfdc",
    "lp.ssoconfig",
    "lp.sfdc.credentials",
    "lp.marketo",
    "lp.marketo.enrichment",
    "lp.marketo.models",
    "lp.apiconsole",
    "lp.managefields",
    "lp.configureattributes"
  ])
  .controller("MainController", function(
    $scope,
    BrowserStorageUtility,
    SessionTimeoutUtility,
    TimestampIntervalUtility,
    LeMessaging,
    Banner,
    Notice,
    Modal,
    ServiceErrorUtility
  ) {
    var previousSession = BrowserStorageUtility.getClientSession();
    var loginDocument = BrowserStorageUtility.getLoginDocument();

    SessionTimeoutUtility.init();

    if (loginDocument && mustUserChangePassword(loginDocument)) {
      window.open("/login", "_self");
    } else if (
      previousSession != null &&
      !SessionTimeoutUtility.hasSessionTimedOut()
    ) {
      //SessionTimeoutUtility.refreshPreviousSession(previousSession.Tenant);
    } else {
      window.open("/login", "_self");
    }

    function mustUserChangePassword(loginDocument, $scope) {
      return (
        loginDocument.MustChangePassword ||
        TimestampIntervalUtility.isTimestampFartherThanNinetyDaysAgo(
          loginDocument.PasswordLastModified
        )
      );
    }

    var ClientSession = BrowserStorageUtility.getClientSession();

    if (ClientSession != null) {
      var LoginDocument = BrowserStorageUtility.getLoginDocument();
      var Tenant = ClientSession ? ClientSession.Tenant : {};

      $scope.userDisplayName = LoginDocument.UserName;
      $scope.tenantName = window.escape(Tenant.DisplayName);
    }

    // Get IE or Edge browser version
    var version = detectIE();

    if (version === false) {
      document.getElementById("body").classList.add("not-ie");
    } else if (version >= 12) {
      document.getElementById("body").classList.add("edge-" + version);
    } else {
      document.getElementById("body").classList.add("ie-" + version);
    }

    // add details to debug result
    console.log(window.navigator.userAgent);
    LeMessaging.subscribe({
      next: message => {
        // console.log("RECEIVED", message);
        if (message.isErrorUtility()) {
          ServiceErrorUtility.process(message.getResponse());
          $scope.$apply(() => {});
        } else {
          switch (message.getPosition()) {
            case BANNER:
              // console.log(message.getMessage());
              Banner[message.getType()]({
                title: message.getMessage(),
                message: message.getFullMessage()
              });
              $scope.$apply(() => {});
              break;

            case MODAL:
              // console.log(message.getMessage());
              Modal[message.getType()]({
                title: message.getMessage(),
                message: message.getFullMessage()
              });
              $scope.$apply(() => {});
              break;

            case NOTIFICATION:
              // console.log(message.getMessage());
              Notice[message.getType()]({
                title: message.getMessage(),
                message: message.getFullMessage()
              });
              $scope.$apply(() => {});
              break;
          }
        }
      }
    });
    /**
     * detect IE
     * returns version of IE or false, if browser is not Internet Explorer
     */
    function detectIE() {
      var ua = window.navigator.userAgent;

      var msie = ua.indexOf("MSIE ");
      if (msie > 0) {
        // IE 10 or older => return version number
        return parseInt(ua.substring(msie + 5, ua.indexOf(".", msie)), 10);
      }

      var trident = ua.indexOf("Trident/");
      if (trident > 0) {
        // IE 11 => return version number
        var rv = ua.indexOf("rv:");
        return parseInt(ua.substring(rv + 3, ua.indexOf(".", rv)), 10);
      }

      var edge = ua.indexOf("Edge/");
      if (edge > 0) {
        // Edge (IE 12+) => return version number
        return parseInt(ua.substring(edge + 5, ua.indexOf(".", edge)), 10);
      }

      // other browser
      return false;
    }
  })
  .factory("LeMessaging", () => {
    return {
      subscribe: observer => {
        messagingService.addSubscriber(observer);
      }
    };
  })
  .factory("LeHTTP", () => {
    return {
      initHeader: headerObj => {
        httpService.setUpHeader(headerObj);
      },
      unsubscribeObservable: observer => {
        httpService.unsubscribeObservable(observer);
      },
      get: (url, observer) => {
        return httpService.get(url, observer);
      }
    };
  })
  // adds Authorization token to $http requests to access API
  .factory("authInterceptor", function(
    $rootScope,
    $q,
    BrowserStorageUtility,
    LeHTTP
  ) {
    return {
      request: function(config) {
        config.headers = config.headers || {};

        if (
          config.headers.Authorization == null &&
          BrowserStorageUtility.getTokenDocument()
        ) {
          config.headers.Authorization = BrowserStorageUtility.getTokenDocument();
          LeHTTP.initHeader({ Authorization: config.headers.Authorization });
        }

        var ClientSession = BrowserStorageUtility.getClientSession();

        if (ClientSession && ClientSession.Tenant) {
          config.headers.TenantId = ClientSession.Tenant.Identifier;
        }

        return config;
      },
      response: function(response) {
        return response || $q.when(response);
      }
    };
  })
  // add authInterceptor factory for Authorization header (above)
  .config(function($httpProvider) {
    $httpProvider.interceptors.push("authInterceptor");
  })
  // prevent $http caching of API results
  .config(function($httpProvider) {
    //initialize get if not there
    if (!$httpProvider.defaults.headers.get) {
      $httpProvider.defaults.headers.get = {};
    }

    //disable IE ajax request caching
    $httpProvider.defaults.headers.get["If-Modified-Since"] =
      "Mon, 26 Jul 1997 05:00:00 GMT";
    $httpProvider.defaults.headers.get["Cache-Control"] = "no-cache";
    $httpProvider.defaults.headers.get["Pragma"] = "no-cache";
  })
  .config(function($animateProvider) {
    $animateProvider.classNameFilter(/ngAnimate/);
  })
  // add escape filter to angular {{ foobar | escape }}
  .filter("escape", function() {
    return window.escape;
  });
