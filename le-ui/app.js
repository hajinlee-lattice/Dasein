"use strict";

/*
       Lattice Engines Express Server Application
    See Gruntfile.js to define environment variables
    See /server/server.js for the actual server code
*/

const Server    = require('./server/server');
const routes    = require('./server/routes');
const routes_d  = require('./server/routes_dist');
const express   = require('express');
const app       = express(); 

// node doesn't support destructuring yet, which would have been nice here.
const options   = {
    ENV:        app.get('env')          || process.env.NODE_ENV || 'production',
    USE_PORT:   process.env.USE_PORT    || 3000,
    API_URL:    process.env.API_URL     || false,
    WHITELIST:  process.env.WHITELIST   || false,
    COMPRESSED: process.env.COMPRESSED  || true,
    HTTPS:      process.env.HTTPS       || false,
    HTTPS_KEY:  process.env.HTTPS_KEY   || '/certs/dev/privatekey.key',
    HTTPS_CRT:  process.env.HTTPS_CRT   || '/certs/dev/certificate.crt',
    LOGGING:    process.env.LOGGING     || true,
    root:       __dirname 
};

if (options['HTTPS'] === 'false') {
    options['HTTPS'] = false;
}

const server = new Server(express, app, options);

options.COMPRESSED === true || options.COMPRESSED === 'true'
    ? server.startLogging('/log') : null;

// when false, API proxy is disabled
options.API_URL && options.API_URL != 'false'
    ? server.useApiProxy(options.API_URL) : null;

// whitelist for proxies
options.WHITELIST && options.API_URL != 'false'
    ? server.trustProxy(options.WHITELIST) : null;

// when files are compressed, strip routing to essential areas only
options.COMPRESSED === true || options.COMPRESSED === 'true'
    ? server.setAppRoutes(routes_d) : server.setAppRoutes(routes);

server.setDefaultRoutes(options.ENV);

module.exports = server.start();