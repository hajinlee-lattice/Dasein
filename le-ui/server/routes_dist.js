"use strict";

module.exports = [
    {
        path: '/projects/common',
        folders: {
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
    }
];