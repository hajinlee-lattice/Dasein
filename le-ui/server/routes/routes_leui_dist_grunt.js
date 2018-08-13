'use strict';

module.exports = [{
    redirect: {
        '/': '/login'
    }
},{
    path: '/projects/common',
    folders: {
        '/app/modules/': '/app/modules/',
        '/components': '/components',
        '/assets': '/assets',
        '/lib': '/lib',
        '/fonts': '/assets/fonts',
        '/images': '/assets/images'
    }
},{
    path: '/projects/login',
    html5mode: true,
    pages: {
        '/login/': 'assets/index.html',
        '/login/index': 'assets/index.html'
    },
    folders: {
        '/login/assets': '/assets',
        '/login/help': '/help',
        '/login': '/assets'
    }
},{
    path: '/projects/prospectdiscovery',
    pages: {
        '/pd/': 'index.html',
        '/pd/index': 'index.html'
    },
    folders: {
        '/pd/app': '/app',
        '/pd/assets': '/assets',
        '/pd/lib': '/lib'
    }
},{
    path: '/projects/atlas',
    html5mode: true,
    pages: {
        '/atlas/': 'assets/index.html',
        '/atlas/index': 'assets/index.html'
    },
    folders: {
        '/atlas/assets': '/assets',
        '/atlas/lib/js': '/lib/js',
        '/atlas': '/assets',
        '/atlas/assets/images': '/assets/images'
    }
},{
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
        '/insights/': 'assets/index.html',
        '/insights/index': 'assets/index.html'
    },
    folders: {
        '/insights': '/assets'
    }
},{
    path: '/projects/dante',
    pages: {
        '/dante/': 'assets/index.html',
        '/dante/index': 'assets/index.html'
    },
    folders: {
        '/dante': '/assets',
        '/dante/CommonAssets': '/assets/CommonAssets',
        '/dante/styles': '/assets/styles',
        '/dante/images': '/assets/images'
    }
},{  
    path: '/projects/demo',
    folders: {
        '/demo/js': '/js'
    }
},{
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
},{
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
}];
