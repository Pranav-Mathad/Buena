import { createRoot } from "react-dom/client";
import App from "./App.tsx";
import { CONFIG_ERROR } from "./lib/config";
import "./index.css";

const root = createRoot(document.getElementById("root")!);

if (CONFIG_ERROR) {
  root.render(
    <main className="min-h-screen flex items-center justify-center p-8">
      <div className="max-w-lg space-y-3">
        <h1 className="text-[22px] font-semibold">Keystone is not configured</h1>
        <p className="text-[15px] text-muted-foreground leading-relaxed">{CONFIG_ERROR}</p>
      </div>
    </main>,
  );
} else {
  root.render(<App />);
}
