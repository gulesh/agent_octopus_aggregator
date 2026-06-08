### How to Run
```
- python -m venv .venv
- source .venv/bin/activate
- pip install -e .
- pip install diaspora-event-sdk certifi kafka-python python-dotenv
```

### Environment Variables
```
cp .env.example .env
# then edit .env with your real credentials
```

> **WARNING: Never commit `.env` — it contains secrets and is gitignored.**

| Variable | Required | Description |
|---|---|---|
| `DIASPORA_SDK_CLIENT_ID` | yes | Diaspora client ID |
| `DIASPORA_SDK_CLIENT_SECRET` | yes | Diaspora client secret |
| `ACADEMY_TUTORIAL_ENDPOINT` | no | Globus Compute endpoint ID; uses local executor if unset |