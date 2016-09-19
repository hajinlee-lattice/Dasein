"use strict";

/*
              Lattice Engines Express Server Application
           See /server/server.js for the actual server code
    See Gruntfile.js to define environment variables for local dev
    See /conf/env/* to define environment variables for QA/PROD/etc
*/
const Server    = require('./server/server');
const express   = require('express');
const app       = express(); 
const ts        = new Date();

const commonOptions = {
    NODE_ENV:   app.get('env')          || process.env.NODE_ENV || 'development',
    HTTPS_KEY:  process.env.HTTPS_KEY   || './server/certs/privatekey.key',
    HTTPS_CRT:  process.env.HTTPS_CRT   || './server/certs/certificate.crt',
    HTTPS_PASS: process.env.HTTPS_PASS  || false,
    WHITELIST:  process.env.WHITELIST   || false,
    COMPRESSED: process.env.COMPRESSED  || false,
    LOGGING:    process.env.LOGGING     || './server/log',
    STACK_ENV:  process.env.STACK_ENV   || 'none',
    TIMESTAMP:  (ts.getMonth()+1)+'/'+ts.getDate()+'/'+ts.getFullYear()+' '+
                ts.getHours()+':'+ts.getMinutes()+':'+ts.getSeconds(),
    APP_ROOT:   __dirname,
    SRC_PATH:   '/projects'
};

// node doesn't support destructuring yet, which would have been nice here.
const options   = Object.assign({
    HTTP_PORT:  process.env.HTTP_PORT   || 3000,
    HTTPS_PORT: process.env.HTTPS_PORT  || false,
    API_URL:    process.env.API_URL     || 'http://app.lattice.local',
    APICON_URL: process.env.APICON_URL  || 'http://localhost:8073',
}, commonOptions);

// force boolean true/false for options
Object.keys(options).forEach(key => {
    options[key] === 'false' ? options[key] = false : null;
    options[key] === 'true'  ? options[key] = true  : null;
});

const routes = require('./server/routes_' + (options.COMPRESSED ? 'dist' : 'dev'));

const server = new Server(express, app, options);

options.LOGGING
    ? server.startLogging(options.LOGGING) : null;

server.setAppRoutes(routes.leui);

// setup API proxy
options.API_URL
    ? server.createApiProxy(options.API_URL) : null;

// setup apiconsole proxy
options.APICON_URL
    ? server.createApiProxy(options.APICON_URL, '/score') : null;

// proxy so clients can download files that need Authorization header
options.API_URL
    ? server.createFileProxy(options.API_URL, '/files', '/pls') : null;

// whitelist for proxies
options.WHITELIST
    ? server.trustProxy(options.WHITELIST) : null;

// for 404/other
server.setDefaultRoutes(options.NODE_ENV);

module.exports = server.start();

// TODO: separate the express apps
if (process.env.NODE_ENV === 'development' || process.env.NODE_ENV === 'qa') {
    const adminOptions = Object.assign({
        HTTP_PORT:  process.env.ADMIN_HTTP_PORT   || 3002,
        HTTPS_PORT: process.env.ADMIN_HTTPS_PORT  || false,
        APIADMIN_URL: process.env.APIADMIN_URL    || 'http://localhost:8085'
    }, commonOptions);

    // force boolean true/false for options
    Object.keys(adminOptions).forEach(key => {
        adminOptions[key] === 'false' ? adminOptions[key] = false : null;
        adminOptions[key] === 'true'  ? adminOptions[key] = true  : null;
    });

    const adminApp = express();
    const adminServer = new Server(express, adminApp, adminOptions);

    adminOptions.LOGGING
       ? adminServer.startLogging(adminOptions.LOGGING) : null;

    adminServer.setAppRoutes(routes.leadmin);

    adminOptions.APIADMIN_URL
        ? adminServer.createApiProxy(adminOptions.APIADMIN_URL, '/admin') : null;

    // whitelist for proxies
    adminOptions.WHITELIST
        ? adminServer.trustProxy(adminOptions.WHITELIST) : null;

    // for 404/other
    adminServer.setDefaultRoutes(adminOptions.NODE_ENV);

    adminServer.start();
}
