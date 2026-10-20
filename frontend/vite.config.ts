import { defineConfig, loadEnv, type Plugin } from "vite";
import react from "@vitejs/plugin-react-swc";
import path from "path";

/**
 * Link previews (LinkedIn, Slack, X) need absolute image URLs, and the deploy
 * URL is only known at build time. When VITE_PUBLIC_URL is set, this adds the
 * og:url and og:image tags pointing at it; otherwise the page has no image tag.
 */
function socialTags(publicUrl: string): Plugin {
  const base = publicUrl.trim().replace(/\/+$/, "");
  return {
    name: "keystone-social-tags",
    transformIndexHtml() {
      if (!base) return [];
      const image = `${base}/og.png`;
      return [
        { tag: "meta", attrs: { property: "og:url", content: `${base}/` }, injectTo: "head" },
        { tag: "meta", attrs: { property: "og:image", content: image }, injectTo: "head" },
        { tag: "meta", attrs: { property: "og:image:width", content: "1200" }, injectTo: "head" },
        { tag: "meta", attrs: { property: "og:image:height", content: "630" }, injectTo: "head" },
        { tag: "meta", attrs: { name: "twitter:image", content: image }, injectTo: "head" },
      ];
    },
  };
}

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "VITE_");
  return {
    server: {
      host: "::",
      port: 8080,
      hmr: {
        overlay: false,
      },
    },
    plugins: [react(), socialTags(env.VITE_PUBLIC_URL ?? "")],
    resolve: {
      alias: {
        "@": path.resolve(__dirname, "./src"),
      },
      dedupe: [
        "react",
        "react-dom",
        "react/jsx-runtime",
        "react/jsx-dev-runtime",
        "@tanstack/react-query",
      ],
    },
  };
});
