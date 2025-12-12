const envBase = (process.env.NEXT_PUBLIC_API_BASE || "").trim();

if (process.env.NODE_ENV === "production" && !envBase) {
  throw new Error(
    "NEXT_PUBLIC_API_BASE is required in production (set to https://mlb-unicorn-engine.onrender.com)"
  );
}

if (!envBase) {
  if (process.env.NODE_ENV !== "production") {
    console.error(
      "NEXT_PUBLIC_API_BASE not set; using default https://mlb-unicorn-engine.onrender.com"
    );
  } else {
    console.error("NEXT_PUBLIC_API_BASE is not set. Please set NEXT_PUBLIC_API_BASE on Vercel.");
  }
}

export const API_BASE = envBase || "https://mlb-unicorn-engine.onrender.com";
