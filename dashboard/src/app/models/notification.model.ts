export interface Notification {
  id: string;
  user_id: string;
  event_type: string;
  title: string;
  body: string;
  metadata: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}
