# Repository Guidelines

## Project Structure & Module Organization
- `lib/remote_persistent_term.ex` defines the core behaviour and `use RemotePersistentTerm` macro.
- `lib/remote_persistent_term/fetcher/` holds fetcher implementations (HTTP, S3, Static) and helpers like HTTP cache logic.
- Tests live in `test/` with paths mirroring modules (e.g., `test/remote_persistent_term/fetcher/http_test.exs`).
- Generated artifacts (`_build/`, `deps/`, `doc/`, `cover/`) are outputs of Mix tasks and should not be edited by hand.

## Build, Test, and Development Commands
- `mix deps.get` installs dependencies.
- `mix compile` builds the library.
- `mix test` runs the full ExUnit suite.
- `mix test test/remote_persistent_term/fetcher/http_test.exs:30` runs a focused test by file and line.
- `mix format` applies the project formatter (see `.formatter.exs`).
- `mix docs` generates ExDoc output into `doc/`.

## Coding Style & Naming Conventions
- Follow `mix format` output (Elixir defaults to 2-space indentation).
- Modules use `CamelCase`; files and functions use `snake_case` (predicates end in `?`).
- Keep option keys consistent with `RemotePersistentTerm` options and fetcher configuration keys.

## Testing Guidelines
- Tests use ExUnit; Mox mocks the ExAws client and Bypass is used for HTTP fetcher tests.
- Prefer deterministic tests and keep external network access mocked or bypassed.
- Name tests with clear behaviour statements; add coverage for new fetcher logic and option validation.

## Commit & Pull Request Guidelines
- Commit messages are short, imperative, and capitalized (e.g., “Improve logs”, “Fix tests”, “Increment version”).
- Keep commits scoped to one change; avoid unrelated refactors in the same commit.
- PRs should include a concise summary, motivation, and the tests you ran (or note if none).
- If a change affects public behaviour or configuration, update documentation and mention it in the PR.

## Notes for Contributors
- CI runs `mix test` on recent OTP/Elixir versions; ensure your changes pass locally before pushing.
- When touching S3 or HTTP fetchers, prefer tests that use Mox/Bypass rather than real services.
