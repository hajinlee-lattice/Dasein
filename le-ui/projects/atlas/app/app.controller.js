import {
    MODAL,
    BANNER,
    NOTIFICATION,
    CLOSE_MODAL
} from '../../common/app/utilities/message';

export default function (
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
    'ngInject';

    var previousSession = BrowserStorageUtility.getClientSession();
    var loginDocument = BrowserStorageUtility.getLoginDocument();

    window.BrowserStorageUtility = BrowserStorageUtility;
    SessionTimeoutUtility.init();

    if (loginDocument && mustUserChangePassword(loginDocument)) {
        window.open('/login', '_self');
    } else if (
        previousSession != null &&
        !SessionTimeoutUtility.hasSessionTimedOut()
    ) {
        //SessionTimeoutUtility.refreshPreviousSession(previousSession.Tenant);
    } else {
        window.open('/login', '_self');
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
        document.getElementById('body').classList.add('not-ie');
    } else if (version >= 12) {
        document.getElementById('body').classList.add('edge-' + version);
    } else {
        document.getElementById('body').classList.add('ie-' + version);
    }

    // add details to debug result
    LeMessaging.subscribe({
        next: message => {
            if (message.isErrorUtility()) {
                ServiceErrorUtility.process(message.getResponse());
                $scope.$apply(() => { });
            } else {
                switch (message.getPosition()) {
                    case BANNER:
                        // console.log(message.getMessage());
                        Banner[message.getType()]({
                            title: message.getMessage(),
                            message: message.getFullMessage()
                        });
                        // $scope.$apply(() => { });
                        break;
                    case CLOSE_MODAL:
                        let modal = Modal.get(message.getName());
                        if (modal) {
                            Modal.modalRemoveFromDOM(modal, { name: message.getName() });
                        }
                        break;
                    case MODAL:
                        // console.log(message.getMessage());
                        Modal[message.getType()]({
                            title: message.getMessage(),
                            icon: message.getIcon() ? message.getIcon() : '',
                            message: message.getFullMessage(),
                            confirmtext: message.getConfirmText() ? message.getConfirmText() : '',
                            dischargetext: message.getDiscardText() ? message.getDiscardText() : ''
                        }, message.getCallbackFn());
                        // $scope.$apply(() => { });
                        break;

                    case NOTIFICATION:
                        // console.log(message.getMessage());
                        Notice[message.getType()]({
                            title: message.getMessage(),
                            message: message.getFullMessage()
                        });
                        // $scope.$apply(() => { });
                        break;

                }
                setTimeout(() => {
                    $scope.$apply(() => { });
                }, 0);
                
            }
        }
    });
    /**
     * detect IE
     * returns version of IE or false, if browser is not Internet Explorer
     */
    function detectIE() {
        var ua = window.navigator.userAgent;

        var msie = ua.indexOf('MSIE ');
        if (msie > 0) {
            // IE 10 or older => return version number
            return parseInt(ua.substring(msie + 5, ua.indexOf('.', msie)), 10);
        }

        var trident = ua.indexOf('Trident/');
        if (trident > 0) {
            // IE 11 => return version number
            var rv = ua.indexOf('rv:');
            return parseInt(ua.substring(rv + 3, ua.indexOf('.', rv)), 10);
        }

        var edge = ua.indexOf('Edge/');
        if (edge > 0) {
            // Edge (IE 12+) => return version number
            return parseInt(ua.substring(edge + 5, ua.indexOf('.', edge)), 10);
        }

        // other browser
        return false;
    }

    //console.log(window.navigator.userAgent);
}
