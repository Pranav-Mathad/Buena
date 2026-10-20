import { cn } from "@/lib/utils";
import { Building2, ChevronDown, type LucideIcon } from "lucide-react";

/** The brand mark: a keystone wedge on a clay tile. Also the favicon. */
export const Mark = ({ className }: { className?: string }) => (
  <svg viewBox="0 0 32 32" aria-hidden="true" className={cn("w-7 h-7 shrink-0", className)}>
    <rect width="32" height="32" rx="8" className="fill-brand" />
    <path d="M8.5 9h15l-2.4 14h-10.2L8.5 9z" className="fill-brand-foreground" />
  </svg>
);

export const Swatch = ({
  Icon = Building2,
  className,
}: {
  Icon?: LucideIcon;
  className?: string;
}) => (
  <span className={cn("swatch shrink-0", className)}>
    <Icon className="w-4 h-4" strokeWidth={1.75} />
  </span>
);

export const Eyebrow = ({
  children,
  className,
}: {
  children: React.ReactNode;
  className?: string;
}) => <span className={cn("eyebrow", className)}>{children}</span>;

export const StatusPill = ({
  variant = "neutral",
  children,
  className,
}: {
  variant?: "active" | "warning" | "muted" | "success" | "neutral" | "brand";
  children: React.ReactNode;
  className?: string;
}) => {
  const dotColor = {
    active: "bg-success",
    success: "bg-success",
    warning: "bg-warning",
    brand: "bg-brand",
    muted: "bg-muted-foreground/60",
    neutral: "bg-muted-foreground/40",
  }[variant];
  return (
    <span
      className={cn(
        "pill bg-card",
        variant === "brand" && "border-brand/30 bg-brand/[0.07] text-brand",
        className,
      )}
    >
      <span className={cn("dot", dotColor)} />
      <span className={variant === "brand" ? "" : "text-foreground"}>{children}</span>
    </span>
  );
};

export const Card = ({ className, children, ...rest }: React.HTMLAttributes<HTMLDivElement>) => (
  <div
    {...rest}
    className={cn("bg-card border border-border rounded-2xl shadow-card transition-all", className)}
  >
    {children}
  </div>
);

export const PrimaryButton = ({
  className,
  children,
  ...rest
}: React.ButtonHTMLAttributes<HTMLButtonElement>) => (
  <button
    {...rest}
    className={cn(
      "inline-flex items-center justify-center gap-2 rounded-full px-6 py-3 text-[14px] font-medium",
      "bg-primary text-primary-foreground transition-all duration-200 ease-out-soft",
      "hover:-translate-y-px hover:shadow-pop active:translate-y-0 active:shadow-none",
      "disabled:opacity-40 disabled:hover:translate-y-0 disabled:hover:shadow-none disabled:cursor-not-allowed",
      className,
    )}
  >
    {children}
  </button>
);

export const SecondaryButton = ({
  className,
  children,
  ...rest
}: React.ButtonHTMLAttributes<HTMLButtonElement>) => (
  <button
    {...rest}
    className={cn(
      "inline-flex items-center justify-center gap-2 rounded-full px-5 py-2.5 text-[14px] font-medium",
      "bg-card border border-border text-foreground transition-all duration-200 ease-out-soft",
      "hover:-translate-y-px hover:border-foreground/25 active:translate-y-0",
      "disabled:opacity-40 disabled:hover:translate-y-0 disabled:cursor-not-allowed",
      className,
    )}
  >
    {children}
  </button>
);

/** A square, borderless button for header and modal actions. */
export const IconButton = ({
  className,
  children,
  ...rest
}: React.ButtonHTMLAttributes<HTMLButtonElement>) => (
  <button
    type="button"
    {...rest}
    className={cn(
      "w-9 h-9 rounded-lg inline-flex items-center justify-center text-muted-foreground",
      "hover:text-foreground hover:bg-foreground/[0.05] transition-colors",
      className,
    )}
  >
    {children}
  </button>
);

/** A native select with the field look and a chevron. */
export const Select = ({
  className,
  children,
  ...rest
}: React.SelectHTMLAttributes<HTMLSelectElement>) => (
  <span className={cn("relative inline-flex min-w-0", className)}>
    <select {...rest} className="field appearance-none pr-9 cursor-pointer truncate">
      {children}
    </select>
    <ChevronDown className="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
  </span>
);

export const Skeleton = ({ className }: { className?: string }) => (
  <div aria-hidden="true" className={cn("skeleton", className)} />
);
