# Resource budget

## T02 tooling profile

T02 introduces only the containerized Maven builder/test tooling. Defaults are intentionally bounded and configurable through `.env` (see `.env.example`):

| Service | CPU limit | Memory limit | Measurement |
|---|---:|---:|---|
| `builder` | 1.0 | 1 GiB | `docker stats --no-stream` while the profile runs |
| `test` | 1.0 | 1 GiB | `docker stats --no-stream` while the profile runs |

The named Maven cache volume is persistent and can be removed with `scripts/kafkanuts.sh reset` or `scripts/kafkanuts.ps1 reset`. These are development guardrails, not production sizing claims. Later services must add measured rows here before their task is marked complete.
