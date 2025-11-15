# Repository Guidelines
## Project Structure & Module Organization
- `main.go` boots the Go backend; core packages live under `api/`, `auth/`, `manager/`, `market/`, `decision/`, and `trader/`, with shared helpers in `middleware/`, `crypto/`, and `logger/`.
- Frontend lives in `web/` (components, pages, hooks), with deployment helpers such as `pm2.config.js` and `docker/`.
- Docs/configs live in `docs/`, `config/`, and `secrets/`, while migrations/logs sit under `migrations/` and `decision_logs/`; keep `CHANGELOG.*` current for behavior changes.
## Build, Test, and Development Commands
- `make build` produces `./nofx`; `make build-frontend` runs `cd web && npm run build`.
- `make test` chains `go test -v ./...` and `cd web && npm run test`; use `make test-backend` / `make test-frontend` to isolate layers.
- `make test-coverage` emits `coverage.html` via `go test -coverprofile`; `make fmt` and `make lint` enforce Go formatting/linting.
- `make run` uses `go run main.go`; `make run-frontend` starts Vite (`cd web && npm run dev`); Docker flows rely on `make docker-build`, `docker-up`, and `docker-down`.
- Frontend helpers: `cd web && npm run lint`, `npm run format`, and `npm run format:check` keep code tidy before commits.

## Coding Style & Naming Conventions
- Go code follows `gofmt`, tabs for indentation, descriptive exports, and `golangci-lint`; keep packages small and errors explicit.
- TypeScript/React in `web/src` uses strict typing, functional components, hooks, and avoids `any`; `eslint` + `prettier` keep JSX/TSX consistent (see `package.json` scripts).
- Branch prefixes (e.g., `feature/`, `fix/`, `docs/`) and file names mirror responsibility (`api/exchange/okx.go`, `web/src/pages/Traders.tsx`).

## Testing Guidelines
- Backend tests live next to production code (`*_test.go`) and should cover API handlers, trading logic, and decision engines; run `go test ./...`.
- Frontend tests rely on Vitest (`web/src/**/*.test.tsx`); run them via `make test-frontend` or `cd web && npm run test`.

## Commit & Pull Request Guidelines
- Use Conventional Commits (`<type>(<scope>): <subject>`, e.g., `fix(api): handle missing trader id`), keep the first line ≤72 characters, present tense, and reference issues/PRs in the body.
- PRs must use `.github/PULL_REQUEST_TEMPLATE.md`, compile (`go build` + `npm run build`), pass lint/tests, complete checklist items, and include testing notes or screenshots for UI changes.
- Keep PRs focused (<300 lines when possible) and rebase on `dev` before requesting review.

## Security & Configuration Tips
- Sensitive settings stay in `secrets/`; copy `config.json.example` to start and inject secrets via environment variables.
- Follow `SECURITY.md` for reporting, avoid hardcoding credentials in `deploy_encryption.sh`, and rotate any keys referenced by `nginx/` or `docker/` scripts.
