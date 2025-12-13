This is a [Next.js](https://nextjs.org) project bootstrapped with [`create-next-app`](https://nextjs.org/docs/app/api-reference/cli/create-next-app).

## Getting Started

First, run the development server:

```bash
npm run dev
# or
yarn dev
# or
pnpm dev
# or
bun dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

You can start editing the page by modifying `app/page.tsx`. The page auto-updates as you edit the file.

This project uses [`next/font`](https://nextjs.org/docs/app/building-your-application/optimizing/fonts) to automatically optimize and load [Geist](https://vercel.com/font), a new font family for Vercel.

## Learn More

To learn more about Next.js, take a look at the following resources:

- [Next.js Documentation](https://nextjs.org/docs) - learn about Next.js features and API.
- [Learn Next.js](https://nextjs.org/learn) - an interactive Next.js tutorial.

You can check out [the Next.js GitHub repository](https://github.com/vercel/next.js) - your feedback and contributions are welcome!

## Deploy on Vercel

The easiest way to deploy your Next.js app is to use the [Vercel Platform](https://vercel.com/new?utm_medium=default-template&filter=next.js&utm_source=create-next-app&utm_campaign=create-next-app-readme) from the creators of Next.js.

Check out our [Next.js deployment documentation](https://nextjs.org/docs/app/building-your-application/deploying) for more details.

### API base configuration

- Set `BACKEND_API_BASE` in Vercel Production/Preview to `https://mlb-unicorn-engine.onrender.com` (or your chosen backend base).
- The browser calls same-origin `GET /api/...` routes on the Vercel domain, which proxy to the backend.
- After changing the env var, trigger a redeploy (disable build cache if you’re troubleshooting).

### Warm endpoint

- `GET /api/warm` pings the backend (`${BACKEND_API_BASE}/api/teams`) so cold starts are less likely.
- Local check (with the dev server running): `curl -fsS http://localhost:3000/api/warm`

### Vercel monorepo note

- Root directory for the Next.js app is `unicorn-website`. A `vercel.json` at repo root points the build and routes there so `/health` and other pages are emitted correctly.
