# Unicorn Website Assets (Static-Only)

The production frontend is built and deployed as a **static-only site** from `static-site/dist/`.

This folder exists for two purposes:

- `unicorn-website/public/data/`: the committed “data product” JSON artifacts (`latest/` + `snapshots/`).
- `unicorn-website/justhtml/`: the JustHTML templates/assets used to generate the HTML/CSS/JS pages.

See `OPERATIONS.md` for the nightly data workflow and rollback steps.
