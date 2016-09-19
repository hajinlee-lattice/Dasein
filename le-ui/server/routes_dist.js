"use strict";

module.exports = {
    leui: [{
        path: '/projects/common',
        folders: {
            '/app/modules/': '/app/modules/',
            '/assets': '/assets',
            '/lib': '/lib',
            '/fonts': '/assets/fonts',
            '/images': '/assets/images'
        }
    },{
        path: '/projects/login',
        pages: {
            '/': 'assets/index.html',
            '/index': 'assets/index.html',
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
        path: '/projects/leadprioritization',
        html5mode: true,
        pages: {
            '/lp/': 'assets/index.html',
            '/lp/index': 'assets/index.html'
        },
        folders: {
            '/lp/assets': '/assets',
            '/lp/lib/js': '/lib/js',
            '/lp': '/assets',
            '/lp/assets/images': '/assets/images'
        }
    },{
        path: '/projects/demo',
        folders: {
            '/demo/js': '/js'
        }
    }],
    leadmin: [{
        path: '/projects/tenantconsole/dist',
        pages: {
            '/': 'index.html',
            '/index': 'index.html',
            '/index.html': 'index.html'
        },
        folders: {
            '/app': '/app',
            '/assets': '/assets',
            '/lib': '/lib',
        }
    }]
};