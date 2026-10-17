import { cn } from "@/lib/utils";
import { Building2, type LucideIcon } from "lucide-react";

export const Swatch = ({ Icon = Building2, className }: { Icon?: LucideIcon; className?: string }) => (
  <span className={cn("swatch shrink-0", className)}>
    <Icon className="w-4 h-4" strokeWidth={1.75} />
  </span>
);

export const StatusPill = ({
  variant = "neutral",
  children,
  className,
}: {
  variant?: "active" | "warning" | "muted" | "success" | "neutral";
  children: React.ReactNode;
  className?: string;
}) => {
  const dotColor = {
    active: "bg-success",
    success: "bg-success",
    warning: "bg-warning",
    muted: "bg-muted-foreground/60",
    neutral: "bg-muted-foreground/40",
  }[variant];
  return (
    <span className={cn("pill bg-card", className)}>
      <span className={cn("dot", dotColor)} />
      <span className="text-foreground">{children}</span>
    </span>
  );
};

export const Card = ({ className, children, ...rest }: React.HTMLAttributes<HTMLDivElement>) => (
  <div
    {...rest}
    className={cn(
      "bg-card border border-border rounded-2xl shadow-card transition-all",
      className,
    )}
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
      "hover:-translate-y-px disabled:opacity-40 disabled:hover:translate-y-0 disabled:cursor-not-allowed",
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
      "bg-transparent border border-border text-foreground transition-all duration-200 ease-out-soft",
      "hover:-translate-y-px hover:bg-foreground/[0.03]",
      className,
    )}
  >
    {children}
  </button>
);
