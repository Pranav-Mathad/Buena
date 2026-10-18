import { useState } from "react";
import { Link } from "react-router-dom";
import { AlertTriangle, HelpCircle, Inbox as InboxIcon, ShieldAlert } from "lucide-react";
import {
  useOpenUncertainties,
  usePendingRejections,
  usePendingSignals,
  useSignalDecision,
} from "@/hooks/queries";
import {
  relativeTime,
  type InboxRejection,
  type InboxSignal,
  type InboxUncertainty,
} from "@/lib/api";
import { Card, PrimaryButton, SecondaryButton, StatusPill } from "@/components/ui-bits";

type Tab = "signals" | "uncertainty" | "rejected";

const Inbox = () => {
  const [tab, setTab] = useState<Tab>("signals");

  const signalsQuery = usePendingSignals();
  const uncertaintiesQuery = useOpenUncertainties();
  const rejectionsQuery = usePendingRejections();

  const signalCount = signalsQuery.data?.length ?? 0;
  const uncertaintyCount = uncertaintiesQuery.data?.length ?? 0;
  const rejectionCount = rejectionsQuery.data?.length ?? 0;

  return (
    <div className="max-w-[1400px] mx-auto px-6 lg:px-10 py-12 animate-fade-in">
      <div className="mb-8 max-w-3xl">
        <div className="flex items-center gap-3">
          <InboxIcon className="w-6 h-6" />
          <h1 className="display text-[40px] tracking-tight">Inbox</h1>
        </div>
        <p className="text-muted-foreground mt-3 text-[15px] leading-relaxed">
          Three trust-layer surfaces. Signals are proposed actions
          waiting on a human. Uncertainty is what we noticed but
          couldn't commit to. Rejections are writes the validator
          refused.
        </p>
      </div>

      <div className="border-b border-border mb-8 flex gap-1">
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
    className={`flex items-center gap-2 px-4 py-2.5 text-[14px] border-b-2 -mb-px transition-colors ${
      active
        ? "border-foreground text-foreground"
        : "border-transparent text-muted-foreground hover:text-foreground"
    }`}
  >
    {icon}
    {label}
    <span
      className={`font-mono text-[11px] px-1.5 py-0.5 rounded ${
        active ? "bg-foreground text-background" : "bg-card border border-border"
      }`}
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
  <Card className="p-10 text-center">
    <p className="text-warning text-[14px]">
      Backend unreachable — try again in a moment.
    </p>
  </Card>
);

const LoadingState = () => (
  <Card className="p-10 text-center">
    <p className="text-muted-foreground text-[14px]">Loading…</p>
  </Card>
);

const PropertyChip = ({
  propertyId,
  propertyName,
}: {
  propertyId: string | null;
  propertyName: string | null;
}) => {
  if (!propertyId || !propertyName) {
    return (
      <span className="pill bg-card text-[12px]">portfolio-level</span>
    );
  }
  return (
    <Link
      to={`/properties/${propertyId}`}
      className="pill bg-card text-[12px] hover:border-foreground/30"
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
    return <EmptyState>No pending signals — your inbox is empty.</EmptyState>;
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
    <Card className="p-5">
      <div className="flex items-start gap-4">
        <SeverityDot severity={s.severity} />
        <div className="flex-1 min-w-0">
          <div className="flex flex-wrap items-baseline gap-2">
            <h3 className="text-[15px] font-semibold">
              {s.type.replace(/_/g, " ")}
            </h3>
            <StatusPill variant={severityVariant(s.severity)}>
              {s.severity}
            </StatusPill>
            <span className="font-mono text-[12px] text-muted-foreground">
              {relativeTime(s.createdAt)}
            </span>
          </div>
          <p className="text-[14px] mt-2 leading-relaxed">{s.message}</p>
          <div className="mt-3">
            <PropertyChip propertyId={s.propertyId} propertyName={s.propertyName} />
          </div>
          <div className="mt-4 flex flex-wrap items-center gap-2">
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
        </div>
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
        No open uncertainties. The extractor was confident enough on
        every recent event to commit a fact.
      </EmptyState>
    );
  return (
    <div className="space-y-3">
      {rows.map((u) => (
        <Card key={u.uncertaintyId} className="p-5">
          <div className="flex items-start gap-4">
            <HelpCircle
              className="w-5 h-5 text-muted-foreground mt-0.5"
              strokeWidth={1.75}
            />
            <div className="flex-1 min-w-0">
              <div className="flex flex-wrap items-baseline gap-2">
                <h3 className="text-[15px] font-semibold">
                  {u.relevantSection?.replace(/_/g, " ") ?? "(unsectioned)"}
                </h3>
                <span className="pill bg-card text-[12px]">
                  via {u.source}
                </span>
                <span className="font-mono text-[12px] text-muted-foreground">
                  {relativeTime(u.createdAt)}
                </span>
              </div>
              <p className="text-[14px] mt-2 leading-relaxed">
                {u.observation}
              </p>
              {u.hypothesis && (
                <p className="text-[13px] italic text-muted-foreground mt-1">
                  hypothesis: {u.hypothesis}
                </p>
              )}
              <p className="text-[12px] text-muted-foreground mt-2">
                <strong>why uncertain:</strong> {u.reasonUncertain}
              </p>
              <div className="mt-3">
                <PropertyChip
                  propertyId={u.propertyId}
                  propertyName={u.propertyName}
                />
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
      <EmptyState>
        No rejected updates. Every recent extraction passed the
        validator.
      </EmptyState>
    );
  return (
    <div className="space-y-3">
      {rows.map((r) => (
        <Card key={r.rejectionId} className="p-5">
          <div className="flex items-start gap-4">
            <ShieldAlert
              className="w-5 h-5 text-warning mt-0.5"
              strokeWidth={1.75}
            />
            <div className="flex-1 min-w-0">
              <div className="flex flex-wrap items-baseline gap-2">
                <h3 className="text-[15px] font-semibold">
                  {r.proposedSection}.{r.proposedField}
                </h3>
                <span className="pill bg-card text-[12px]">
                  {r.constraintName.replace(/_/g, " ")}
                </span>
                <span className="font-mono text-[12px] text-muted-foreground">
                  {relativeTime(r.createdAt)}
                </span>
              </div>
              <p className="text-[14px] mt-2 leading-relaxed">
                <span className="text-muted-foreground">proposed:</span>{" "}
                {r.proposedValue}
              </p>
              <p className="text-[13px] italic text-warning mt-1">
                rejected: {r.reason}
              </p>
              {r.snippet && (
                <p className="text-[12px] text-muted-foreground mt-2 whitespace-pre-line border-l-2 border-border pl-3">
                  {r.snippet}
                </p>
              )}
              <div className="mt-3">
                <PropertyChip
                  propertyId={r.propertyId}
                  propertyName={r.propertyName}
                />
              </div>
            </div>
          </div>
        </Card>
      ))}
    </div>
  );
};

const SeverityDot = ({ severity }: { severity: string }) => {
  const color =
    severity === "urgent"
      ? "bg-warning"
      : severity === "high"
      ? "bg-warning/70"
      : severity === "medium"
      ? "bg-foreground/50"
      : "bg-muted-foreground/40";
  return <span className={`dot mt-1.5 ${color}`} />;
};

function severityVariant(severity: string) {
  if (severity === "urgent" || severity === "high") return "warning";
  if (severity === "medium") return "neutral";
  return "muted";
}

export default Inbox;
