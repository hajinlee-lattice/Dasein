//import { actions, reducer } from './playlistchannels.redux.js';

export default function ($stateProvider) {
    'ngInject';
    $stateProvider
        .state('home.playbook.listchannels', {
            url: '/playlistchannels',
            params: {
                pageTitle: 'playbook List',
                pageIcon: 'ico-segments',
                edit: null
            },
            // onEnter: function (ReduxService, $state) {
            //     ReduxService.connect(
            //         'playlistchannels',
            //         actions,
            //         reducer,
            //         $state.get('home.playlistchannels')
            //     );
            // },
            // onExit: function ($state) {
            //     $state.get('home.playlistchannels').data.redux.unsubscribe();
            // },
            views: {
                "summary@": {
                    templateUrl: 'app/navigation/summary/BlankLine.html'
                },
                "main@": 'atlasPlaylistchannels'
            }
        });
};