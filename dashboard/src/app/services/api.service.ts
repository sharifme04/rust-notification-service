import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { Notification } from '../models/notification.model';

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
}
