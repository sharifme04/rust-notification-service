import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { Notification } from '../models/notification.model';

export interface UserPreferences {
  user_id: string;
  email: string | null;
  webhook_url: string | null;
}

export interface UpsertPreferences {
  email: string | null;
  webhook_url: string | null;
  channels: string[];
}

@Injectable({ providedIn: 'root' })
export class ApiService {
  private readonly baseUrl = '/api/v1';

  constructor(private http: HttpClient) {}

  listNotifications(): Observable<Notification[]> {
    return this.http.get<Notification[]>(`${this.baseUrl}/notifications`);
  }

  createNotification(payload: Partial<Notification>): Observable<Notification> {
    return this.http.post<Notification>(`${this.baseUrl}/notifications`, payload);
  }

  getPreferences(): Observable<UserPreferences> {
    return this.http.get<UserPreferences>(`${this.baseUrl}/preferences`);
  }

  upsertPreferences(body: UpsertPreferences): Observable<UserPreferences> {
    return this.http.put<UserPreferences>(`${this.baseUrl}/preferences`, body);
  }
}
