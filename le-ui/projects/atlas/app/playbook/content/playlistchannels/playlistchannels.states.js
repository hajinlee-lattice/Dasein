import PlaylistChannelsComponent from './playlistchannels.component';
import ReactRouter from 'atlas/react/router';

const playlistChannels = {
    parent: 'home',
    name: "playlistChannels",
    url: "/listchannels",
    resolve: [{
        token: 'PlaybookWizardStore',
        resolveFn: () => {
            return ReactRouter.getRouter()['ngservices'].PlaybookWizardStore;
        }
    }],
    views: {
        'main@': PlaylistChannelsComponent
    }
};

const playlistchannelsstate = [playlistChannels];
export default playlistchannelsstate;