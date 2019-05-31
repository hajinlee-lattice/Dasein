'use strict';

module.exports = [{
    redirect: {
        '/': '/login'
    }
}, {
    path: '/projects/common',
    folders: {
        '/app': '/app',
        '/components': '/components',
        '/lib': '/lib',
        '/assets': '/assets',
        '/fonts': '/assets/fonts',
        '/images': '/assets/images'
    }
}, {
    path: '/projects/login',
    html5mode: true,
    xframe_options: 'deny',
    pages: {
        '/login': '/dist/indexwp.html',
        '/login/form': '/dist/indexwp.html',
        '/login/old': 'index.html',
        '/login/old/index': 'index.html'
    },
    folders: {
        '/login': '/dist',
        '/dist': '/dist',
        '/login/app': '/app',
        '/login/assets': '/assets',
        '/login/lib': '/lib',
        '/login/help': '/help'
    }
}, {
    path: '/projects/atlas',
    pages: {
        '/pd/': 'index.html',
        '/pd/index': 'index.html'
    },
    folders: {
        '/pd/app': '/app',
        '/pd/assets': '/assets',
        '/pd/lib': '/lib'
    }
}, {
    path: '/projects/atlas',
    html5mode: true,
    pages: {
        '/atlas/': '/dist/indexwp.html',
        '/atlas/index': '/dist/indexwp.html',
        '/atlas/old': 'assets/index.html',
        '/atlas/old/index': 'assets/index.html'
    },
    folders: {
        '/atlas': '/dist',
        '/dist': '/dist',
        '/atlas/app': '/app',
        '/atlas/assets': '/assets',
        '/atlas/lib': '/lib',
        '/atlas/assets': '/assets',
        '/atlas/lib/js': '/lib/js',
    }
}, {
    path: '/projects/lpi',
    html5mode: true,
    pages: {
        '/lpi/': '/dist/indexwp.html',
        '/lpi/index': '/dist/indexwp.html',
        '/lpi/old': 'assets/index.html',
        '/lpi/old/index': 'assets/index.html'
    },
    folders: {
        '/lpi': '/dist',
        '/dist': '/dist',
        '/lpi/app': '/app',
        '/lpi/assets': '/assets',
        '/lpi/lib': '/lib',
        '/lpi/assets': '/assets',
        '/lpi/lib/js': '/lib/js',
    }
}, {
    path: '/projects/insights',
    html5mode: true,
    xframe_allow: [
        'localhost:3000',
        'localhost:3001',
        'localhost:3002',
        'localhost:3003',
        'force.com',
        'salesforce.com',
        'lattice-engines.com'
    ],
    pages: {
        '/insights/': '/dist/indexwp.html',
        '/insights': '/dist/indexwp.html',
        '/insights/index': '/dist/indexwp.html'
    },
    folders: {
        '/': '/dist',
        '/dist': '/dist',
        '/insights': '/dist',
    }
}, {
    path: '/projects/dante',
    pages: {
        '/dante/': 'assets/index.html',
        '/dante/index': 'assets/index.html'
    },
    folders: {
        '/dante': '/assets',
        '/dante/CommonAssets': '/assets/CommonAssets',
        '/dante/resources': '/assets/resources',
        '/dante/styles': '/assets/styles',
        '/dante/images': '/assets/images'
    }
}, {
    path: '/projects/demo',
    folders: {
        '/demo/js': '/js'
    }
}, {
    path: '/projects/lp2',
    pages: {
        '/lp2/': 'index.html',
        '/lp2/index': 'index.html'
    },
    folders: {
        '/lp2/app': '/app',
        '/lp2/assets': '/assets',
        '/lp2/lib': '/lib'
    }
}, {
    path: '/projects/websocket',
    pages: {
        '/websocket/': 'index.html',
        '/websocket/index': 'index.html'
    },
    folders: {
        '/websocket/app': '/app',
        '/websocket/assets': '/assets',
        '/websocket/lib': '/lib',
    }
}, {
    path: '/projects/uicomponents',
    html5mode: true,
    pages: {
        '/uicomponents/': '/storybook-static/index.html',
        '/uicomponents/index': '/storybook-static/index.html'
    },
    folders: {
        '/uicomponents': '/storybook-static'
    }
}];
