# Speed to Lead — RTP

Real-time speed-to-lead dashboard for Rooftop Power Co.

## Deploy to Railway

1. Push this folder to a GitHub repo
2. In Railway: New Project → Deploy from GitHub → select repo
3. Railway auto-detects Node.js and deploys
4. Go to Settings → Networking → Generate Domain
5. Your app is live at `https://your-app.up.railway.app`

## GHL Webhook Setup

### New Lead Trigger
- In GHL: Automation → New Workflow
- Trigger: "Contact Created" (or "Form Submitted")
- Action: "Send Webhook"
- URL: `https://your-domain.up.railway.app/webhook/lead-in`
- Method: POST
- Body (map GHL fields):
```json
{
  "contact_id": "{{contact.id}}",
  "first_name": "{{contact.first_name}}",
  "last_name": "{{contact.last_name}}",
  "phone": "{{contact.phone}}",
  "source": "{{contact.source}}"
}
```

### Call Made Trigger
- Trigger: "Outbound Call" or "Call Status Changed"
- Action: "Send Webhook"
- URL: `https://your-domain.up.railway.app/webhook/call-made`
- Method: POST
- Body:
```json
{
  "contact_id": "{{contact.id}}",
  "call_status": "{{call.status}}"
}
```

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | /webhook/lead-in | New lead arrives |
| POST | /webhook/call-made | Call placed to lead |
| GET | /api/leads | Get all current leads |
| POST | /api/mark-called/:id | Manually mark as called |
| POST | /api/clear | Clear all leads |
| GET | /events | SSE stream for real-time updates |

## Timer Color Logic

| Color | Time | Status |
|-------|------|--------|
| Green | 0-60s | On fire |
| Yellow | 1-3 min | Good |
| Orange | 3-5 min | Urgent |
| Red (flashing) | 5+ min | Critical |
