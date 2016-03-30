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

// node doesn't support destructuring yet, which would have been nice here.
const options   = {
    NODE_ENV:   app.get('env')          || process.env.NODE_ENV || 'production',
    HTTP_PORT:  process.env.HTTP_PORT   || false,
    HTTPS_PORT: process.env.HTTPS_PORT  || 3000,
    HTTPS_KEY:  process.env.HTTPS_KEY   || './server/certs/privatekey.key',
    HTTPS_CRT:  process.env.HTTPS_CRT   || './server/certs/certificate.crt',
    HTTPS_PASS: process.env.HTTPS_PASS  || false,
    API_URL:    process.env.API_URL     || false,
    WHITELIST:  process.env.WHITELIST   || false,
    COMPRESSED: process.env.COMPRESSED  || true,
    LOGGING:    process.env.LOGGING     || '/var/log/ledp',
    TIMESTAMP:  new Date().getTime(),
    APP_ROOT:   __dirname 
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

// when false, API proxy is disabled
options.API_URL
    ? server.useApiProxy(options.API_URL) : null;

// whitelist for proxies
options.WHITELIST
    ? server.trustProxy(options.WHITELIST) : null;

server.setAppRoutes(routes);

// for 404/other
server.setDefaultRoutes(options.NODE_ENV);

module.exports = server.start();