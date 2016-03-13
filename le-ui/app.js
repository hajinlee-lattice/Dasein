"use strict";

/*
       Lattice Engines Express Server Application
    See Gruntfile.js to define environment variables
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
    API_URL:    process.env.API_URL     || 'http://bodcdevhdpweb52.dev.lattice.local:8080',
    WHITELIST:  process.env.WHITELIST   || false,
    COMPRESSED: process.env.COMPRESSED  || true,
    root:       __dirname 
}

const server = new Server(express, app, options);

server.startLogging('/log');

// when false, API proxy is disabled
options.API_URL === true || options.API_URL === 'true'
    ? server.useApiProxy(options.API_URL) : null;

// whitelist for proxies
options.WHITELIST === true || options.WHITELIST === 'true'
    ? server.trustProxy(options.WHITELIST) : null;

// when files are compressed, strip routing to essential areas only
options.COMPRESSED === true || options.COMPRESSED === 'true'
    ? server.setAppRoutes(routes_d) : server.setAppRoutes(routes);

server.setDefaultRoutes(options.ENV);

module.exports = server.start();