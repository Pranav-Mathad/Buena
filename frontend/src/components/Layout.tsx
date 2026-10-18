import { NavLink, Outlet } from "react-router-dom";
import { Moon, Sun } from "lucide-react";
import { useTheme } from "@/hooks/use-theme";
import { NotificationsBell } from "./NotificationsBell";

const navItems = [
  { to: "/", label: "Portfolio", end: true },
  { to: "/properties/509393da-6806-49ef-9e59-3da0213008cd", label: "Properties" },
  { to: "/inbox", label: "Inbox" },
  { to: "/compare", label: "Compare" },
  { to: "/draft-reply", label: "Drafts" },
];

export const Layout = () => {
  const { theme, toggle } = useTheme();
  return (
    <div className="min-h-screen flex flex-col">
      <header className="sticky top-0 z-40 border-b border-border bg-background/85 backdrop-blur-sm">
        <div className="max-w-[1400px] mx-auto px-6 lg:px-10 h-16 flex items-center justify-between">
          <NavLink to="/" className="display text-[18px] tracking-tight">
            Keystone
          </NavLink>
          <nav className="hidden md:flex items-center gap-1">
            {navItems.map((item) => (
              <NavLink
                key={item.to}
                to={item.to}
                end={item.end}
                className={({ isActive }) =>
                  `px-3 py-2 text-[14px] rounded-lg transition-colors ${
                    isActive ? "text-foreground" : "text-muted-foreground hover:text-foreground"
                  }`
                }
              >
                {item.label}
              </NavLink>
            ))}
          </nav>
          <div className="flex items-center gap-2">
            <div className="hidden sm:flex items-center gap-2 text-[13px] text-muted-foreground mr-1">
              <span className="dot bg-success animate-pulse-soft" />
              <span className="font-mono">live · Buena</span>
            </div>
            <NotificationsBell />
            <button
              onClick={toggle}
              aria-label="Toggle theme"
              className="w-9 h-9 rounded-lg flex items-center justify-center hover:bg-foreground/[0.04] transition-colors"
            >
              {theme === "light" ? <Moon className="w-4 h-4" /> : <Sun className="w-4 h-4" />}
            </button>
          </div>
        </div>
      </header>
      <main className="flex-1">
        <Outlet />
      </main>
      <footer className="border-t border-border mt-20">
        <div className="max-w-[1400px] mx-auto px-6 lg:px-10 py-6 text-[13px] text-muted-foreground">
          Built for the Buena hackathon track · Keystone · 2026
        </div>
      </footer>
    </div>
  );
};
