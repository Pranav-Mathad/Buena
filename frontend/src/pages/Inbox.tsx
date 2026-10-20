import { useState } from "react";
import { Link } from "react-router-dom";
import { AlertTriangle, HelpCircle, Inbox as InboxIcon, ShieldAlert } from "lucide-react";
import {
  useOpenUncertainties,
  usePendingRejections,
  usePendingSignals,
  useSignalDecision,
} from "@/hooks/queries";
import { useDocumentTitle } from "@/hooks/use-document-title";
import {
  relativeTime,
  type InboxRejection,
  type InboxSignal,
  type InboxUncertainty,
} from "@/lib/api";
import { cn } from "@/lib/utils";
import {
  Card,
  Eyebrow,
  PrimaryButton,
  SecondaryButton,
  Skeleton,
  StatusPill,
  Swatch,
} from "@/components/ui-bits";

type Tab = "signals" | "uncertainty" | "rejected";

const Inbox = () => {
  useDocumentTitle("Inbox");
  const [tab, setTab] = useState<Tab>("signals");

  const signalsQuery = usePendingSignals();
  const uncertaintiesQuery = useOpenUncertainties();
  const rejectionsQuery = usePendingRejections();

  const signalCount = signalsQuery.data?.length ?? 0;
  const uncertaintyCount = uncertaintiesQuery.data?.length ?? 0;
  const rejectionCount = rejectionsQuery.data?.length ?? 0;

  return (
    <div className="max-w-[1400px] mx-auto px-5 sm:px-6 lg:px-10 py-10 lg:py-12 animate-fade-in">
      <div className="mb-8 max-w-3xl">
        <Eyebrow>Review</Eyebrow>
        <div className="mt-3 flex items-center gap-3">
          <Swatch Icon={InboxIcon} className="!w-10 !h-10 rounded-xl" />
          <h1 className="display text-[clamp(30px,4vw,40px)]">Inbox</h1>
        </div>
        <p className="text-muted-foreground mt-4 text-[15px] leading-relaxed">
          Signals are proposed actions waiting for a decision. Uncertainties are observations the
          extractor could not confirm. Rejected updates are writes the validator refused.
        </p>
      </div>

      <div className="border-b border-border mb-6 flex gap-1 overflow-x-auto">
        <TabButton
          active={tab === "signals"}
          icon={<AlertTriangle className="w-3.5 h-3.5" />}
          label="Signals"
          count={signalCount}
          onClick={() => setTab("signals")}
        />
        <TabButton
          active={tab === "uncertainty"}
          icon={<HelpCircle className="w-3.5 h-3.5" />}
          label="Uncertainty"
          count={uncertaintyCount}
          onClick={() => setTab("uncertainty")}
        />
        <TabButton
          active={tab === "rejected"}
          icon={<ShieldAlert className="w-3.5 h-3.5" />}
          label="Rejected"
          count={rejectionCount}
          onClick={() => setTab("rejected")}
        />
      </div>

      <div className="max-w-4xl">
        {tab === "signals" && (
          <SignalsList
            loading={signalsQuery.isLoading}
            error={signalsQuery.isError}
            rows={signalsQuery.data ?? []}
          />
        )}
        {tab === "uncertainty" && (
          <UncertaintyList
            loading={uncertaintiesQuery.isLoading}
            error={uncertaintiesQuery.isError}
            rows={uncertaintiesQuery.data ?? []}
          />
        )}
        {tab === "rejected" && (
          <RejectionsList
            loading={rejectionsQuery.isLoading}
            error={rejectionsQuery.isError}
            rows={rejectionsQuery.data ?? []}
          />
        )}
      </div>
    </div>
  );
};

const TabButton = ({
  active,
  icon,
  label,
  count,
  onClick,
}: {
  active: boolean;
  icon: React.ReactNode;
  label: string;
  count: number;
  onClick: () => void;
}) => (
  <button
    onClick={onClick}
    aria-pressed={active}
    className={cn(
      "flex items-center gap-2 px-3.5 py-2.5 text-[14px] border-b-2 -mb-px whitespace-nowrap transition-colors",
      active
        ? "border-foreground text-foreground font-medium"
        : "border-transparent text-muted-foreground hover:text-foreground",
    )}
  >
    {icon}
    {label}
    <span
      className={cn(
        "font-mono text-[11px] min-w-[20px] px-1.5 py-0.5 rounded-full text-center",
        active ? "bg-foreground text-background" : "bg-card border border-border",
      )}
    >
      {count}
    </span>
  </button>
);

const EmptyState = ({ children }: { children: React.ReactNode }) => (
  <Card className="p-10 text-center">
    <p className="text-muted-foreground text-[14px]">{children}</p>
  </Card>
);

const ErrorState = () => (
  <Card className="p-10 text-center border-warning/40">
    <p className="text-warning text-[14px] font-medium">
      Backend unreachable. Try again in a moment.
    </p>
  </Card>
);

const LoadingState = () => (
  <div className="space-y-3">
    <Skeleton className="h-[150px] rounded-2xl" />
    <Skeleton className="h-[150px] rounded-2xl" />
  </div>
);

const PropertyChip = ({
  propertyId,
  propertyName,
}: {
  propertyId: string | null;
  propertyName: string | null;
}) => {
  if (!propertyId || !propertyName) {
    return <span className="pill bg-card font-normal">portfolio-level</span>;
  }
  return (
    <Link
      to={`/properties/${propertyId}`}
      className="pill bg-card font-normal hover:border-foreground/30 transition-colors"
    >
      {propertyName}
    </Link>
  );
};

const SignalsList = ({
  loading,
  error,
  rows,
}: {
  loading: boolean;
  error: boolean;
  rows: InboxSignal[] | undefined;
}) => {
  if (loading) return <LoadingState />;
  if (error) return <ErrorState />;
  if (!rows || rows.length === 0)
    return <EmptyState>No pending signals. Your inbox is empty.</EmptyState>;
  return (
    <div className="space-y-3">
      {rows.map((s) => (
        <SignalCard key={s.id} signal={s} />
      ))}
    </div>
  );
};

const SignalCard = ({ signal: s }: { signal: InboxSignal }) => {
  const decision = useSignalDecision();
  const busy = decision.isPending;
  return (
    <Card className="relative overflow-hidden p-5 sm:p-6">
      <span className={cn("absolute left-0 inset-y-0 w-[3px]", severityBar(s.severity))} />
      <div className="flex flex-wrap items-center gap-2">
        <h3 className="text-[15px] font-semibold first-letter:uppercase">
          {s.type.replace(/_/g, " ")}
        </h3>
        <StatusPill variant={severityVariant(s.severity)}>{s.severity}</StatusPill>
        <span className="font-mono text-[12px] text-muted-foreground">
          {relativeTime(s.createdAt)}
        </span>
      </div>
      <p className="text-[14.5px] mt-2.5 leading-relaxed">{s.message}</p>
      <div className="mt-3">
        <PropertyChip propertyId={s.propertyId} propertyName={s.propertyName} />
      </div>
      <div className="mt-5 flex flex-wrap items-center gap-2">
        <PrimaryButton
          className="px-4 py-2 text-[13px]"
          disabled={busy}
          onClick={() => decision.mutate({ id: s.id, decision: "approve" })}
        >
          Approve
        </PrimaryButton>
        <SecondaryButton
          className="px-4 py-2 text-[13px]"
          disabled={busy}
          onClick={() => decision.mutate({ id: s.id, decision: "reject" })}
        >
          Reject
        </SecondaryButton>
        {decision.isError && (
          <span role="alert" className="text-[13px] text-destructive">
            {decision.error.message}
          </span>
        )}
      </div>
    </Card>
  );
};

const UncertaintyList = ({
  loading,
  error,
  rows,
}: {
  loading: boolean;
  error: boolean;
  rows: InboxUncertainty[];
}) => {
  if (loading) return <LoadingState />;
  if (error) return <ErrorState />;
  if (rows.length === 0)
    return (
      <EmptyState>
        No open uncertainties. Every recent extraction was committed as a fact.
      </EmptyState>
    );
  return (
    <div className="space-y-3">
      {rows.map((u) => (
        <Card key={u.uncertaintyId} className="p-5 sm:p-6">
          <div className="flex items-start gap-4">
            <HelpCircle className="w-5 h-5 text-muted-foreground mt-0.5" strokeWidth={1.75} />
            <div className="flex-1 min-w-0">
              <div className="flex flex-wrap items-center gap-2">
                <h3 className="text-[15px] font-semibold first-letter:uppercase">
                  {u.relevantSection?.replace(/_/g, " ") ?? "(unsectioned)"}
                </h3>
                <span className="pill bg-card font-normal">via {u.source}</span>
                <span className="font-mono text-[12px] text-muted-foreground">
                  {relativeTime(u.createdAt)}
                </span>
              </div>
              <p className="text-[14.5px] mt-2.5 leading-relaxed">{u.observation}</p>
              {u.hypothesis && (
                <p className="text-[13px] italic text-muted-foreground mt-1">
                  hypothesis: {u.hypothesis}
                </p>
              )}
              <p className="text-[12.5px] text-muted-foreground mt-2">
                <strong className="font-medium text-foreground">why uncertain:</strong>{" "}
                {u.reasonUncertain}
              </p>
              <div className="mt-3">
                <PropertyChip propertyId={u.propertyId} propertyName={u.propertyName} />
              </div>
            </div>
          </div>
        </Card>
      ))}
    </div>
  );
};

const RejectionsList = ({
  loading,
  error,
  rows,
}: {
  loading: boolean;
  error: boolean;
  rows: InboxRejection[];
}) => {
  if (loading) return <LoadingState />;
  if (error) return <ErrorState />;
  if (rows.length === 0)
    return (
      <EmptyState>No rejected updates. Every recent extraction passed the validator.</EmptyState>
    );
  return (
    <div className="space-y-3">
      {rows.map((r) => (
        <Card key={r.rejectionId} className="p-5 sm:p-6">
          <div className="flex items-start gap-4">
            <ShieldAlert className="w-5 h-5 text-warning mt-0.5" strokeWidth={1.75} />
            <div className="flex-1 min-w-0">
              <div className="flex flex-wrap items-center gap-2">
                <h3 className="text-[15px] font-semibold font-mono">
                  {r.proposedSection}.{r.proposedField}
                </h3>
                <span className="pill bg-card font-normal">
                  {r.constraintName.replace(/_/g, " ")}
                </span>
                <span className="font-mono text-[12px] text-muted-foreground">
                  {relativeTime(r.createdAt)}
                </span>
              </div>
              <p className="text-[14.5px] mt-2.5 leading-relaxed">
                <span className="text-muted-foreground">proposed:</span> {r.proposedValue}
              </p>
              <p className="text-[13px] italic text-warning mt-1">rejected: {r.reason}</p>
              {r.snippet && (
                <p className="text-[12.5px] text-muted-foreground mt-2 whitespace-pre-line border-l-2 border-border pl-3">
                  {r.snippet}
                </p>
              )}
              <div className="mt-3">
                <PropertyChip propertyId={r.propertyId} propertyName={r.propertyName} />
              </div>
            </div>
          </div>
        </Card>
      ))}
    </div>
  );
};

function severityBar(severity: string) {
  if (severity === "urgent") return "bg-warning";
  if (severity === "high") return "bg-warning/60";
  if (severity === "medium") return "bg-foreground/30";
  return "bg-border";
}

function severityVariant(severity: string) {
  if (severity === "urgent" || severity === "high") return "warning";
  if (severity === "medium") return "neutral";
  return "muted";
}

export default Inbox;
