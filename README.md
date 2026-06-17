### How to Run
```
- python -m venv .venv
- source .venv/bin/activate
- pip install -e .
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

### How to get Diaspora Id and Secret the first time
```
- Login to [Globus compute][https://www.globus.org/compute]
- Go to Settings > Developers > Advance Registration
- Follow the prompt to create a new project
- At the end of this process you will have both the ID and Secret that you can use for the above environment variables.
```