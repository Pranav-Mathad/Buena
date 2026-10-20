import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react-swc";
import path from "path";

export default defineConfig({
  plugins: [react()],
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["./src/test/setup.ts"],
    include: ["src/**/*.{test,spec}.{ts,tsx}"],
    env: { VITE_API_BASE_URL: "http://api.test" },
  },
  resolve: {
    alias: { "@": path.resolve(__dirname, "./src") },
  },
});
