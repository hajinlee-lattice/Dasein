export default function($transitions) {
    'ngInject';

    // setup StateHistory service and close any error banners left open
    $transitions.onStart({}, function(trans) {
        var StateHistory = trans.injector().get('StateHistory'),
            Banner = trans.injector().get('Banner'),
            from = trans.$from(),
            to = trans.$to();

        StateHistory.setFrom(from, trans.params('from'));
        StateHistory.setTo(to, trans.params('to'));

        if (from.name !== to.name) {
            var fromSplit = from.name.split('.');
            var toSplit = to.name.split('.');
            var fromCheck = fromSplit
                .splice(1, toSplit.length >> 1 || 1)
                .join('.');
            var toCheck = toSplit.splice(1, toSplit.length >> 1 || 1).join('.');
            var delay = fromCheck === toCheck ? 7500 : 0;

            Banner.reset(delay);
        }
    });

    // when user hits browser Back button after app instantiate, send back to login
    $transitions.onStart(
        {
            to: 'home',
            from: function(state) {
                return (
                    state.name == 'home.models' ||
                    state.name == 'home.datacloud'
                );
            }
        },
        function(trans) {
            window.open('/login', '_self');
        }
    );

    // ShowSpinner when transitioning states that alter main ui-view
    $transitions.onStart(
        {
            to: function(state) {
                return state.views['main'] || state.views['main@'];
            }
        },
        function(trans) {
            var params = trans.params('to') || {},
                from = trans.$from(),
                to = trans.$to();

            if (to.name !== from.name && params.LoadingSpinner !== false) {
                ShowSpinner(params.LoadingText || '');
            }
        }
    );

    $transitions.onBefore({}, function(trans) {
        var BrowserStorageUtility = trans
                .injector()
                .get('BrowserStorageUtility'),
            ClientSession = BrowserStorageUtility.getClientSession(),
            stateService = trans.router.stateService,
            params = Object.assign({}, trans.params('to')),
            tenant = ClientSession.Tenant;

        if (params.tenantName === '') {
            params.tenantName = tenant.DisplayName;

            return stateService.target(trans.to(), params);
        }
    });

    $transitions.onSuccess({ to: 'home' }, function(trans) {
        var BrowserStorageUtility = trans
                .injector()
                .get('BrowserStorageUtility'),
            ClientSession = BrowserStorageUtility.getClientSession(),
            stateService = trans.router.stateService,
            tenant = ClientSession.Tenant;

        if (trans.$to().params.tenantName != tenant.DisplayName) {
            var FeatureFlags = trans.injector().get('FeatureFlagService');

            FeatureFlags.GetAllFlags().then(function(result) {
                var flags = FeatureFlags.Flags(),
                    sref = FeatureFlags.FlagIsEnabled(flags.ENABLE_CDL)
                        ? 'home.segment.explorer.attributes'
                        : 'home.models';

                trans.router.stateService.go(sref, {
                    tenantName: tenant.DisplayName,
                    segment: 'Create'
                });
            });
        }
    });
}
