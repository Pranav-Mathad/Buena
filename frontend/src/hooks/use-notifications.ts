import { createContext, useContext } from "react";

export type Notification = {
  id: string;
  eventId: string;
  propertyId: string | null;
  propertyName: string;
  category: string;
  description: string;
  factsWritten: number;
  receivedAt: number;
  readAt: number | null;
};

export type NotificationContextValue = {
  notifications: Notification[];
  unreadCount: number;
  markAllRead: () => void;
  clear: () => void;
};

export const NotificationContext = createContext<NotificationContextValue | null>(null);

export const useNotifications = (): NotificationContextValue => {
  const ctx = useContext(NotificationContext);
  if (!ctx) {
    throw new Error("useNotifications must be used inside NotificationProvider");
  }
  return ctx;
};
