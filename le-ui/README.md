For Linux:

sudo apt-get install ruby-full
sudo gem install sass
sudo apt-get install npm
sudo npm install -g npm@latest
npm install -g n@latest
sudo ln -s /usr/local/n/versions/node/<VERSION>/bin/node /usr/bin/node


Installation:
-----------------------------------
'npm install' - install production dependencies, generate sass

Development:
-----------------------------------
'npm run InstallDev' - fetch devDependencies
'npm run qa' - start server for QA API, sass sentry
'npm run dev' - start server for localhost API, sass sentry
'npm run prod' - start server for production with no api proxy