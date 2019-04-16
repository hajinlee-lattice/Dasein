import { actions, reducer } from './segmentation.redux.js';

export default function ($stateProvider) {
    'ngInject';
    $stateProvider
        .state('home.segmentation', {
            url: '/segmentation',
            params: {
                pageTitle: 'Segmentation List',
                pageIcon: 'ico-segments',
                edit: null
            },
            onEnter: function (ReduxService, $state) {
                ReduxService.connect(
                    'segmentation',
                    actions,
                    reducer,
                    $state.get('home.segmentation')
                );
            },
            onExit: function ($state) {
                $state.get('home.segmentation').data.redux.unsubscribe();
            },
            views: {
                "summary@": {
                    templateUrl: 'app/navigation/summary/BlankLine.html'
                },
                "main@": 'atlasSegmentation'
            }
        });
};