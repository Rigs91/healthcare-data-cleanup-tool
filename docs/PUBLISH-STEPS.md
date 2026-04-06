# Publish Steps

## Recommended Repo Name

`healthcare-data-cleanup-tool`

## GitHub About Text

`Guided healthcare dataset cleanup with deterministic transforms and optional local Ollama planning.`

## Suggested Website Field

Leave the website blank unless you add a short demo video, GIF walkthrough, or portfolio landing page.

## GitHub Topics

- `healthcare`
- `healthcare-data`
- `data-quality`
- `data-cleaning`
- `data-normalization`
- `fastapi`
- `ollama`
- `local-llm`
- `synthetic-data`
- `workflow-ui`

## Manual Publish Commands

```powershell
gh repo create healthcare-data-cleanup-tool --public --source . --remote origin --push
gh repo edit --description "Guided healthcare dataset cleanup with deterministic transforms and optional local Ollama planning."
gh repo edit --add-topic healthcare --add-topic healthcare-data --add-topic data-quality --add-topic data-cleaning --add-topic data-normalization --add-topic fastapi --add-topic ollama --add-topic local-llm --add-topic synthetic-data --add-topic workflow-ui
```

## Pre-Publish Check

- Run `.\RUN_HCDATA_1_CLICK.cmd`
- Run `.\scripts\run_tests.ps1`
- Run `.\scripts\test_tool.ps1 -BaseUrl http://127.0.0.1:8000` or the active launcher URL
- Confirm screenshots in `docs/screenshots/`
- Confirm no `.env` values or local artifacts are staged
