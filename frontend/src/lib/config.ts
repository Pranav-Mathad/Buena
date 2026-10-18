// Runtime configuration read from Vite's build-time environment.
//
// VITE_API_BASE_URL is required: without it every request would go to a guessed
// origin and fail in confusing ways, so main.tsx shows CONFIG_ERROR instead of
// the app when it is missing.

const raw = import.meta.env.VITE_API_BASE_URL;

export const CONFIG_ERROR: string | null =
  raw && raw.trim()
    ? null
    : "VITE_API_BASE_URL is not set. Copy frontend/.env.example to frontend/.env.local " +
      "(or pass it as a build argument) and restart the dev server.";

/** Base URL of the Keystone API, without a trailing slash. */
export const API_BASE_URL: string = (raw ?? "").trim().replace(/\/+$/, "");
