#!/bin/bash

sudo debconf-set-selections <<< 'mysql-server mysql-server/root_password password welcome'
sudo debconf-set-selections <<< 'mysql-server mysql-server/root_password_again password welcome'
sudo apt-get -y install mysql-server-5.6
