import Component from './playlistchannels.component.js';
import Config from './playlistchannels.config.js';

const Dependencies = [];

angular
    .module('atlas.playlistchannels', Dependencies)
    .config(Config)
    .component('atlasPlaylistchannels', Component);