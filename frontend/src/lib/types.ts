// Domain types shared by the API client and the pages.

export type Property = {
  id: string;
  name: string;
  address: string;
  stats: { events: number; facts: number; needsReview: number };
  status: "active";
  openIssues: number;
  isHero?: boolean;
};

export type Fact = {
  value: string;
  source: string;
  date: string;
  dateIso?: string;
  urgent?: boolean;
  reason?: string;
  eventId?: string;
  factId?: string;
  humanEdited?: boolean;
  editedBy?: string | null;
};

export type Section = {
  title: string;
  facts: Fact[];
  isUncertain?: boolean;
  isContext?: boolean;
};
