export const API_BASE = process.env.NEXT_PUBLIC_API_BASE as string | undefined;

if (process.env.NODE_ENV === 'production' && (!API_BASE || API_BASE.length === 0)) {
  throw new Error('NEXT_PUBLIC_API_BASE is not set. Please set NEXT_PUBLIC_API_BASE on Vercel.');
}

if (!API_BASE || API_BASE.length === 0) {
  console.error('NEXT_PUBLIC_API_BASE is not set. Using relative API routes may fail.');
}
