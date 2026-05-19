import { Component } from '@angular/core';
import { NotificationFeedComponent } from './components/notification-feed/notification-feed.component';
import { NotificationListComponent } from './components/notification-list/notification-list.component';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [NotificationFeedComponent, NotificationListComponent],
  template: `
    <div style="max-width: 960px; margin: 32px auto; padding: 0 16px;">
      <h1>Notification Dashboard</h1>
      <app-notification-feed></app-notification-feed>
      <app-notification-list></app-notification-list>
    </div>
  `,
})
export class AppComponent {}
