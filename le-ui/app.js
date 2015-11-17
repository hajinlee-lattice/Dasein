"use strict";

/*
       Lattice Engines Express Server Application
    See Gruntfile.js to define environment variables
*/

const Server    = require('./server/server');
const routes    = require('./server/routes');
const express   = require('express');

const app       = express(); 

// node doesn't support destructuring yet, which would have been nice here.
const options   = {
    ENV:        app.get('env')          || 'development',
    USE_PORT:   process.env.USE_PORT    || 3000,
    API_URL:    process.env.API_URL     || false,
    PROXY_IP:   process.env.PROXY_IP    || false,
    WHITELIST:  process.env.WHITELIST   || false,
    root:       __dirname 
}

const server = new Server(express, app, options);

options.API_URL     
    ? server.useApiProxy(options.API_URL) : null;

options.WHITELIST
    ? server.trustProxy(options.WHITELIST) : null;

server.setAppRoutes(routes);
server.setDefaultRoutes(options.ENV);

module.exports = server.start();