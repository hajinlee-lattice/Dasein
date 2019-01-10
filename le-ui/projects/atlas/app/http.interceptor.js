export default {
    Config: $httpProvider => {
        'ngInject';

        //initialize get if not there
        if (!$httpProvider.defaults.headers.get) {
            $httpProvider.defaults.headers.get = {};
        }

        //disable IE ajax request caching
        $httpProvider.defaults.headers.get['If-Modified-Since'] =
            'Mon, 26 Jul 1997 05:00:00 GMT';
        $httpProvider.defaults.headers.get['Cache-Control'] = 'no-cache';
        $httpProvider.defaults.headers.get['Pragma'] = 'no-cache';
    },
    Interceptor: ($q, BrowserStorageUtility) => {
        'ngInject';

        return {
            request: function(config) {
                config.headers = config.headers || {};

                var ClientSession = BrowserStorageUtility.getClientSession();

                if (BrowserStorageUtility.getTokenDocument()) {
                    config.headers.Authorization = BrowserStorageUtility.getTokenDocument();
                }

                if (ClientSession && ClientSession.Tenant) {
                    config.headers.TenantId = ClientSession.Tenant.Identifier;
                }

                return config;
            },
            response: function(response) {
                return response || $q.when(response);
            }
        };
    },
    InterceptorConfig: $httpProvider => {
        'ngInject';

        $httpProvider.interceptors.push('authInterceptor');
    },
    AxiosAuthorization: (LeHTTP, BrowserStorageUtility) => {
        'ngInject';

        if (BrowserStorageUtility.getTokenDocument()) {
            let ClientSession = BrowserStorageUtility.getClientSession();
            let Authorization = BrowserStorageUtility.getTokenDocument();
            let TenantId = ClientSession.Tenant.Identifier;

            LeHTTP.initHeader({
                Authorization: Authorization,
                TenantId: TenantId
            });
        }
    }
};
