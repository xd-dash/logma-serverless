# ChatGPT continuation wake-ups

This bootstrap system lets a GitHub Action finish without keeping an agent turn open. The final workflow step publishes a typed event through logma-serverless. A local browser resumer watches that event over SSE and submits a continuation into the original ChatGPT chat when its URL is available.

## Event path

1. Start the local resumer before dispatching the long-running workflow.
2. Give the workflow an unguessable channel such as `chatgpt:continuation:<random-uuid>` and the current `https://chatgpt.com/...` conversation URL.
3. Let the workflow run independently.
4. Its final `if: always()` step posts a continuation to `POST /continuations`.
5. logma-serverless validates the event and publishes the existing `PublishRequest` wire shape to Redis.
6. `/events?channel=...` relays it over SSE.
7. The resumer opens the original URL and submits the continuation. If no trusted URL was supplied, it starts a new chat with the action context.

Redis Pub/Sub is intentionally the live wake-up transport, not durable storage. The composite action retries delivery to logma-serverless, but the resumer must already be connected. A production successor should use a Redis Stream or another acknowledged queue and record completed event IDs durably.

## Run the resumer

```bash
cd tools/chatgpt-resumer
npm install
LOGMA_URL='https://your-logma-serverless.example' \
LOGMA_REDIS_AUTH='...' \
CONTINUATION_CHANNEL="chatgpt:continuation:$(uuidgen | tr '[:upper:]' '[:lower:]')" \
npm start
```

The resumer uses `.chatgpt-bootstrap-profile` by default. Sign in to ChatGPT in the Chrome window the first time it starts. Do not upload or commit that profile.

## Publish from a workflow

```yaml
- name: Wake ChatGPT continuation
  if: ${{ always() }}
  uses: xd-dash/logma-serverless/.github/actions/publish-continuation@claude/main
  with:
    endpoint: ${{ secrets.LOGMA_SERVERLESS_URL }}
    redis-auth: ${{ secrets.LOGMA_REDISCLI_AUTH }}
    channel: ${{ inputs.continuation_channel }}
    chat-url: ${{ inputs.chat_url }}
    status: ${{ job.status }}
    summary: Build and verification finished; inspect the run before changing code.
    prompt: Review this completed workflow, inspect its logs and artifacts, and continue the original task from the verified result.
```

Both `continuation_channel` and `chat_url` should be optional workflow inputs so ordinary runs remain independent of this personal bootstrap path. The channel is a capability identifier; generate a fresh high-entropy value per waiting chat and do not print the Redis credential.

## Security boundary

- `POST /continuations` and `GET /events` retain the existing `X-Rediscli-Auth` middleware.
- Only `https://chatgpt.com` continuation URLs are accepted by both server and client.
- Events expire at ingress after 24 hours and prompts are limited to 32 KiB.
- Action summaries, logs, and artifact contents are untrusted data. The generated prompt explicitly prevents them from becoming instructions.
- The persistent browser profile stays on the local machine; logma-serverless never receives ChatGPT cookies.
