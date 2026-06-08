### How to Run
```
- python -m venv .venv
- source .venv/bin/activate
- pip install -e .
- pip install diaspora-event-sdk certifi kafka-python python-dotenv
```

### Environment Variables
Copy `.env` and fill in your credentials before running:
```
DIASPORA_SDK_CLIENT_ID=your_client_id
DIASPORA_SDK_CLIENT_SECRET=your_client_secret
# ACADEMY_TUTORIAL_ENDPOINT=your_endpoint  # optional, uses local executor if unset
```
`.env` is gitignored and must not be committed.