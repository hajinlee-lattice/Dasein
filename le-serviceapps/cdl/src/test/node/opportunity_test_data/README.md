# Opportunity Test Data Generator
Generator opportunity data for e2e tests

## Installation

#### 1. Install NodeJS

Recommend to install with [nvm](https://github.com/creationix/nvm).

1. Run `node -v` first, if you see `v11.7.0` or above, skip this section.
2. Run `nvm`, if you have this command, skip step 3.
3. Run `curl -o- https://raw.githubusercontent.com/creationix/nvm/v0.34.0/install.sh | bash` to install nvm.
4. Might need to restart terminal after installing nvm. Run `nvm install 11.7.0` to install NodeJS.
5. Run `nvm alias default 11.7.0` to set version `11.7.0` to default.
6. Run `node -v` and make sure you see `v11.7.0` in terminal

#### 2. Install Dependencies

Run the following commands to install required libraries.

`npm install`

#### 3. Basic Usage

- Use `npm run generate <NUM_RECORDS>` to generate test data with `<NUM_RECORDS>` rows
- Add flag `-i` to generate incremental test case, otherwise generate first load test case
- Add flag `-d <DAYS>` to configure how many days of data to generate, default is `90` days
- Generated data will be located at `./data/opportunity_<UUID>.csv`, with a stats file `./data/opportunity_<UUID>.json` containing the expected metrics for each account/stage

#### 4. Test Cases

- First load test case generate data ending at `2017-07-01` to simulate user loading data the first time.
- Incremental test case generate data ending at `2017-08-01` to simulate user loading new data after first load, keeping an one month gap between first load.
