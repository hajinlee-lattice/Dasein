"use strict";

const root = '/projects';

module.exports = [
    {
        path: root + '/common',
        static: {
            '/app': '/app',
            '/assets': '/assets',
            '/lib': '/lib'
        }
    },{
        path: root + '/login',
        render: 'index.html',
        routes: [
            '/',
            '/index',
            '/login/',
            '/login/index'
        ],
        static: {
            '/login/app': '/app',
            '/login/assets': '/assets',
            '/login/lib': '/lib',
            '/login/help': '/help'
        }
    },{
        path: root + '/prospectdiscovery',
        render: 'index.html',
        routes: [
            '/pd/',
            '/pd/index'
        ],
        static: {
            '/pd/app': '/app',
            '/pd/assets': '/assets',
            '/pd/lib': '/lib'
        }
    },{
        path: root + '/leadprioritization',
        render: 'index.html',
        routes: [
            '/lp/',
            '/lp/index'
        ],
        static: {
            '/lp/app': '/app',
            '/lp/assets': '/assets',
            '/lp/lib': '/lib'
        }
    },{
        path: root + '/demo',
        render: 'index.html',
        routes: [
            '/demo/',
            '/demo/index'
        ],
        static: {
            '/demo/app': '/app',
            '/demo/assets': '/assets',
            '/demo/lib': '/lib'
        }
    }
];