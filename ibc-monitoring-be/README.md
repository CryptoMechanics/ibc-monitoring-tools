## IBC Monitoring Backend

Indexes actions relating to the IBC Wraplock and Wraptoken contracts from multiple supported Antelope chains into a Postgresql database. It gathers data from the `get_actions` Hyperion API at a user-configurable interval, and is fork-tolerant. It also maintains a table reflecting the health of the Hyperion API endpoints.

It periodically matches the actions between chains to determine any instances where:

1) There has been an issue, withdraw or cancel action - requiring a proof - without a corresponding event on the source chain
2) The corresponding source and destination actions have different owner, beneficiary or quantity values

In such an event, it triggers a discrepancy notification as follows:

*(to be decided)*

It also presents a basic HTTP API for viewing the matched and unmatched actions:

```
/transfers?fmt=json&start=2023-02-08T00:00:00&end=2023-02-09T00:00:00
/transfers-summary?fmt=html&start=2023-02-08T00:00:00&end=2023-02-09T00:00:00
```

Complete API documentation may be found at `/docs`

### Quickstart

1) Install docker and docker-compose
2) Create the config.env file like the template, and modify as required
3) Edit the chains.json file to include information for all chains being monitored
4) Run using `docker-compose up`

### How to stop

`docker-compose down`

### Configuration

The `config.env`  file stores basic configuration option:

```
POSTGRES_USER, POSTGRES_PASSWORD and POSTGRES_DB - credentials for postgresql database

ACTION_COLLECTION_START_TIME - chain date/time (e.g. yyyy-mm-dd) at which to start collecting wraplock/wraptoken actions from Hyperion API
ACTION_COLLECTION_QUERY_INTERVAL_SECONDS - how often to poll Hyperion API for recent actions
ACTION_COLLECTION_REPOPULATION_QUERY_INTERVAL_SECONDS - how often to poll Hyperion API for historic actions when catching up

MATCHING_START_TIME - chain date/time (e.g. yyyy-mm-dd) at which to start matching actions and checks for discrepancies
MATCHING_INTERVAL_SECONDS=5 - how often to carry out the action matching process

LOGGING_LEVEL - `DEBUG`, `INFO` or `ERROR` depending on the level of detail
```

The `chains.json` file stores the details of the Hyperion API endpoints for each chain, and the wraplock/token contract names for which actions are collected.

### TODO

- add appropriate notification code using websockets, telegram, etc.
  - check that there there can never be false positives for such discrepancies