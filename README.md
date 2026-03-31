
<h1 align="center">
  <br>
  <a href="https://openpecha.org"><img src="https://avatars.githubusercontent.com/u/82142807?s=400&u=19e108a15566f3a1449bafb03b8dd706a72aebcd&v=4" alt="OpenPecha" width="150"></a>
  <br>
</h1>

<!-- Replace with 1-sentence description about what this tool is or does.-->

<h3 align="center">Use this repo template for all new Python projects.</h3>

## Description

Project description goes here.

## Project owner(s)

<!-- Link to the repo owners' github profiles -->

- [@10zinten](https://github.com/10zinten)
- [@evanyerburgh](https://github.com/evanyerburgh)

## Integrations

<!-- Add any intregrations here or delete `- []()` and write None-->

None
## Docs

<!-- Update the link to the docs -->

Read the docs [here](https://wiki.openpecha.org/#/dev/coding-guidelines).

## Cloudflare Crawl Pipeline

This repo includes a Python pipeline that submits Cloudflare Browser Rendering
crawl jobs, polls each job to completion, downloads discovered assets locally,
and writes one JSON output per target site.

### 1) Configure credentials

Set Cloudflare credentials as environment variables (do not hardcode secrets):

```bash
export CF_ACCOUNT_ID="your_account_id"
export CF_API_TOKEN="your_cloudflare_api_token"
```

### 2) Provide target websites

Put one URL per line in `data/sites.txt`:

```text
https://www.gyutolibrary.com/
https://example.com/
```

### 3) Run the pipeline

```bash
python scripts/cloudflare_crawl_pipeline.py \
  --sites-file data/sites.txt \
  --output-dir output \
  --formats html markdown \
  --depth 10 \
  --limit 1000 \
  --poll-interval 10 \
  --max-poll-minutes 90 \
  --concurrency 2
```

### 4) Output layout

For each site, the pipeline writes:

- `output/<site_slug>/crawl_result.json`
- `output/<site_slug>/assets/*` (downloaded files referenced from crawled pages)
- `output/<site_slug>/checkpoint.json` (resume/progress metadata)

It also writes `output/summary.json` with status for all submitted sites.
