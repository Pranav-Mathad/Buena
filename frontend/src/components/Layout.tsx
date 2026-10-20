import { useEffect, useState } from "react";
import { NavLink, Outlet, useLocation } from "react-router-dom";
import { Github, Menu, Moon, Sun, X } from "lucide-react";
import { useTheme } from "@/hooks/use-theme";
import { API_BASE_URL, REPO_URL } from "@/lib/config";
import { cn } from "@/lib/utils";
import { NotificationsBell } from "./NotificationsBell";
import { IconButton, Mark } from "./ui-bits";

type NavItem = { to: string; label: string; end?: boolean };

const iconLinkClass =
  "w-9 h-9 rounded-lg inline-flex items-center justify-center text-muted-foreground hover:text-foreground hover:bg-foreground/[0.05] transition-colors";

export const Layout = () => {
  const { theme, toggle } = useTheme();
  const { pathname } = useLocation();
  const [menuOpen, setMenuOpen] = useState(false);

  // The phone menu closes once a link in it has navigated.
  useEffect(() => {
    setMenuOpen(false);
  }, [pathname]);

  const navItems: NavItem[] = [
    { to: "/", label: "Portfolio", end: true },
    { to: "/inbox", label: "Inbox" },
    { to: "/search", label: "Search" },
    { to: "/draft-reply", label: "Drafts" },
  ];

  return (
    <div className="min-h-screen flex flex-col">
      <header className="sticky top-0 z-40 border-b border-border bg-background/80 backdrop-blur-md">
        <div className="max-w-[1400px] mx-auto px-5 sm:px-6 lg:px-10 h-14 flex items-center justify-between gap-4">
          <NavLink
            to="/"
            className="flex items-center gap-2.5 shrink-0 rounded-md"
            aria-label="Keystone, portfolio"
          >
            <Mark className="w-6 h-6" />
            <span className="display text-[17px]">Keystone</span>
          </NavLink>

          <nav className="hidden md:flex items-center gap-0.5" aria-label="Primary">
            {navItems.map((item) => (
              <NavLink
                key={item.to}
                to={item.to}
                end={item.end}
                className={({ isActive }) =>
                  cn(
                    "px-3 py-1.5 text-[13.5px] font-medium rounded-full transition-colors",
                    isActive
                      ? "bg-foreground/[0.06] text-foreground"
                      : "text-muted-foreground hover:text-foreground hover:bg-foreground/[0.04]",
                  )
                }
              >
                {item.label}
              </NavLink>
            ))}
          </nav>

          <div className="flex items-center gap-0.5">
            {REPO_URL && (
              <a
                href={REPO_URL}
                target="_blank"
                rel="noreferrer"
                aria-label="Source on GitHub"
                title="Source on GitHub"
                className={iconLinkClass}
              >
                <Github className="w-4 h-4" />
              </a>
            )}
            <NotificationsBell />
            <IconButton
              onClick={toggle}
              aria-label={theme === "light" ? "Switch to dark theme" : "Switch to light theme"}
            >
              {theme === "light" ? <Moon className="w-4 h-4" /> : <Sun className="w-4 h-4" />}
            </IconButton>
            <IconButton
              className="md:hidden"
              onClick={() => setMenuOpen((open) => !open)}
              aria-label={menuOpen ? "Close menu" : "Open menu"}
              aria-expanded={menuOpen}
              aria-controls="mobile-nav"
            >
              {menuOpen ? <X className="w-4 h-4" /> : <Menu className="w-4 h-4" />}
            </IconButton>
          </div>
        </div>

        {menuOpen && (
          <nav
            id="mobile-nav"
            aria-label="Primary"
            className="md:hidden border-t border-border bg-background animate-slide-down"
          >
            <div className="px-3 py-2 flex flex-col">
              {navItems.map((item) => (
                <NavLink
                  key={item.to}
                  to={item.to}
                  end={item.end}
                  className={({ isActive }) =>
                    cn(
                      "px-3 py-2.5 rounded-lg text-[15px] transition-colors",
                      isActive
                        ? "bg-foreground/[0.06] text-foreground font-medium"
                        : "text-muted-foreground hover:text-foreground",
                    )
                  }
                >
                  {item.label}
                </NavLink>
              ))}
            </div>
          </nav>
        )}
      </header>

      <main className="flex-1">
        <Outlet />
      </main>

      <footer className="border-t border-border mt-16">
        <div className="max-w-[1400px] mx-auto px-5 sm:px-6 lg:px-10 py-7 flex flex-wrap items-center justify-between gap-x-6 gap-y-3 text-[13px] text-muted-foreground">
          <div className="flex items-center gap-2">
            <Mark className="w-4 h-4" />
            <span>Keystone</span>
          </div>
          <div className="flex items-center gap-5">
            <a href={`${API_BASE_URL}/docs`} className="hover:text-foreground transition-colors">
              API docs
            </a>
            {REPO_URL && (
              <a
                href={REPO_URL}
                target="_blank"
                rel="noreferrer"
                className="hover:text-foreground transition-colors"
              >
                Source
              </a>
            )}
          </div>
        </div>
      </footer>
    </div>
  );
};
