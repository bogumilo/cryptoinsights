## Cryptoinsights
This repo contains simple CLI app that connect to public [Coinbase Market
Data](https://docs.cloud.coinbase.com/exchange/docs/websocket-overview)
Websocket feed. You don't need to authenticate with Coinbase API key to
use it.

## Setup
```bash
git clone https://github.com/bogumilo/dockerbogumilo/cryptoinsights && cd
cryptoinsights
```
Python environment:
```bash
pynev virtualenv 3.9.12 env
pyenv activate env
pip install -r requirements.txt
```

 ## Run script and wait >15min to ingest enough data
