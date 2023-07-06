# tap-shopify

`tap-shopify` is a Singer tap for Shopify.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.


## Installation

Install from GitHub:

```bash
pipx install git+https://github.com/sehnem/tap-shopify.git@master
```


## Configuration

### Accepted Config Options

## Capabilities

* `catalog`
* `state`
* `discover`
* `about`
* `stream-maps`
* `schema-flattening`
* `batch`

## Settings

| Setting             | Required | Default | Description |
|:--------------------|:--------:|:-------:|:------------|
| auth_token          | True     | None    | The token to authenticate against the API service |
| shop                | True     | None    | The shopify shop name |
| api_version         | False    | 2023-04 | The version of the API to use |
| start_date          | False    | None    | The earliest record date to sync |
| bulk                | False    | false   | To use the bulk API or not |
| stream_maps         | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config   | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled  | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth| False    | None    | The max depth to flatten schemas. |
| batch_config        | False    | None    |             |

A full list of supported settings and capabilities is available by running: `tap-shopify --about`

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-shopify --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

To get the access token for your store use the folowing [guide](https://shopify.dev/docs/apps/auth/admin-app-access-tokens).

## Usage

You can easily run `tap-shopify` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-shopify --version
tap-shopify --help
tap-shopify --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-shopify` CLI interface directly using `poetry run`:

```bash
poetry run tap-shopify --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-shopify
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-shopify --version
# OR run a test `elt` pipeline:
meltano elt tap-shopify target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
