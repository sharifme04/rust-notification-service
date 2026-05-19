import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ApiService } from '../../services/api.service';
import { Notification } from '../../models/notification.model';

@Component({
  selector: 'app-notification-list',
  standalone: true,
  imports: [CommonModule],
  template: `
    <h2>History</h2>
    <table>
      <thead>
        <tr><th>When</th><th>Event</th><th>Title</th></tr>
      </thead>
      <tbody>
        <tr *ngFor="let n of notifications">
          <td>{{ n.created_at }}</td>
          <td>{{ n.event_type }}</td>
          <td>{{ n.title }}</td>
        </tr>
      </tbody>
    </table>
  `,
})
export class NotificationListComponent implements OnInit {
  notifications: Notification[] = [];
  constructor(private api: ApiService) {}

  ngOnInit(): void {
    this.api.listNotifications().subscribe((n) => (this.notifications = n));
  }
}
