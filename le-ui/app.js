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
    ENV:        app.get('env')          || 'production',
    USE_PORT:   process.env.USE_PORT    || 3000,
    API_URL:    process.env.API_URL     || 'http://bodcdevhdpweb52.dev.lattice.local:8080',
    WHITELIST:  process.env.WHITELIST   || false,
    root:       __dirname 
}

const server = new Server(express, app, options);

server.startLogging('/log');

options.API_URL
    ? server.useApiProxy(options.API_URL) : null;

options.WHITELIST
    ? server.trustProxy(options.WHITELIST) : null;

options.ENV == 'qa'
    ? server.setAppRoutes(routes) : server.setAppRoutes(routes_d);

server.setDefaultRoutes(options.ENV);

module.exports = server.start();