'use strict';

/*
              Lattice Engines Express Server Application
           See /server/server.js for the actual server code
    See Gruntfile.js to define environment variables for local dev
    See /conf/env/* to define environment variables for QA/PROD/etc
*/
const express   = require('express');
const chalk     = require('chalk');

const DateUtil  = require('./server/utilities/DateUtil');
const Server    = require('./server/Server');

const configs   = require('./server/configs/config');
const apps      = process.env.NODE_APPS.split(',');

apps.forEach(app => {
    new Server(express, express(), configs[app]).start();
});


process.on('uncaughtException', err => {
    console.log(chalk.red(DateUtil.getTimeStamp() + ':uncaughtException>')+'\n', err);
});

process.on('SIGTERM', err => {
    console.log(chalk.red(DateUtil.getTimeStamp() + ':SIGTERM>'), err);
});

process.on('ECONNRESET', err => {
    console.log(chalk.red(DateUtil.getTimeStamp() + ':ECONNRESET>'), err);
});
