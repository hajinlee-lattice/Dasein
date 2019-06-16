import { actions, reducer } from './attributes.redux.js';

const Dependencies = [
    'common.attributes.header',
    'common.attributes.subheader',
    'common.attributes.controls',
    'common.attributes.filters',
    'common.attributes.categories',
    'common.attributes.list',
    'common.attributes.enable',
    'common.attributes.activate',
    'common.attributes.edit'
];

const States = $stateProvider => {
    $stateProvider.state('home.attributes', {
        url: '/attributes',
        params: {
            pageIcon: 'ico-analysis',
            pageTitle: 'Attribute Admin'
        },
        onEnter: (ReduxService, $state, $stateParams) => {
            // console.log($state.get('home.attributes'));
            ReduxService.connect(
                'attributes',
                actions,
                reducer,
                $state.get('home.attributes')
            );
            // console.log('onEnter', $state.get('home.attributes'));
        },
        onExit: $state => {
            // console.log('onExit', $state.get('home.attributes'));
            $state.get('home.attributes').data.redux.unsubscribe();
        },
        resolve: {
            tabs: () => [
                {
                    sref: 'home.attributes.activate',
                    label: 'Activate Premium Attributes'
                },
                {
                    sref: 'home.attributes.enable',
                    label: 'Enable & Disable Attributes'
                },
                {
                    sref: 'home.attributes.edit',
                    label: 'Edit Name & Description'
                }
            ]
        },
        views: {
            'summary@': 'attrHeader'
        },
        redirectTo: 'home.attributes.activate'
    });
};

angular.module('common.attributes', Dependencies).config(States);
