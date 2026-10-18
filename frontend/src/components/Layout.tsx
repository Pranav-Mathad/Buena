import { NavLink, Outlet } from "react-router-dom";
import { Moon, Sun } from "lucide-react";
import { useFeaturedProperty } from "@/hooks/queries";
import { API_BASE_URL } from "@/lib/config";
import { useTheme } from "@/hooks/use-theme";
import { NotificationsBell } from "./NotificationsBell";

type NavItem = { to: string; label: string; end?: boolean };

export const Layout = () => {
  const { theme, toggle } = useTheme();
  const featured = useFeaturedProperty();
  // "Properties" opens the featured property (FEATURED_PROPERTY on the API);
  // the link is omitted until one is known.
  const navItems: NavItem[] = [
    { to: "/", label: "Portfolio", end: true },
    ...(featured ? [{ to: `/properties/${featured.id}`, label: "Properties" }] : []),
    { to: "/inbox", label: "Inbox" },
    { to: "/compare", label: "Compare" },
    { to: "/draft-reply", label: "Drafts" },
  ];
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
          Keystone ·{" "}
          <a
            href={`${API_BASE_URL}/docs`}
            className="underline underline-offset-2 hover:text-foreground"
          >
            API docs
          </a>
        </div>
      </footer>
    </div>
  );
};
