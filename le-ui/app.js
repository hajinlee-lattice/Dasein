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

// node doesn't support destructuring yet, which would have been nice here.
const options   = {
    NODE_ENV:   app.get('env')          || process.env.NODE_ENV || 'development',
    HTTP_PORT:  process.env.HTTP_PORT   || 3000,
    HTTPS_PORT: process.env.HTTPS_PORT  || false,
    HTTPS_KEY:  process.env.HTTPS_KEY   || './server/certs/privatekey.key',
    HTTPS_CRT:  process.env.HTTPS_CRT   || './server/certs/certificate.crt',
    HTTPS_PASS: process.env.HTTPS_PASS  || false,
    API_URL:    process.env.API_URL     || 'http://app.lattice.local',
    APICON_URL: process.env.APICON_URL  || 'http://localhost:8073',
    RM_URL:     process.env.RM_URL      || 'http://localhost:8088',
    WHITELIST:  process.env.WHITELIST   || false,
    COMPRESSED: process.env.COMPRESSED  || false,
    LOGGING:    process.env.LOGGING     || './server/log',
    STACK_ENV:  process.env.STACK_ENV   || 'none',
    TIMESTAMP:  (ts.getMonth()+1)+'/'+ts.getDate()+'/'+ts.getFullYear()+' '+
                ts.getHours()+':'+ts.getMinutes()+':'+ts.getSeconds(),
    APP_ROOT:   __dirname,
    SRC_PATH:   '/projects'
};

// force boolean true/false for options
Object.keys(options).forEach(key => {
    options[key] === 'false' ? options[key] = false : null;
    options[key] === 'true'  ? options[key] = true  : null;
});

const routes = require('./server/routes_' + (options.COMPRESSED ? 'dist' : 'dev'));

const server = new Server(express, app, options);

options.LOGGING
    ? server.startLogging(options.LOGGING) : null;

// setup API proxy
options.API_URL
    ? server.createApiProxy(options.API_URL) : null;

// setup apiconsole proxy
options.APICON_URL
    ? server.createApiProxy(options.APICON_URL, '/score') : null;

app.all('/cluster/app/*', function (req, res, next) {
    res.redirect(301, options.RM_URL + req.url);
});

// proxy so clients can download files that need Authorization header
options.API_URL
    ? server.createFileProxy(options.API_URL, '/files', '/pls') : null;

// whitelist for proxies
options.WHITELIST
    ? server.trustProxy(options.WHITELIST) : null;

server.setAppRoutes(routes);

// for 404/other
server.setDefaultRoutes(options.NODE_ENV);

module.exports = server.start();