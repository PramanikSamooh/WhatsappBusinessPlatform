# OneHub → WhatsApp Platform cutover guide

How to move OneHub's WhatsApp sends off the **direct Meta Cloud API** and onto
**this platform's** `POST /api/whatsapp/send`, so every outbound message is
recorded and student replies keep context (no more generic "any question about
course?" replies to a birthday or webinar message).

Do this on a maintenance day. The two systems share the **same WABA phone
number**, which is why replies already land on this platform's webhook.

> **OTP stays direct.** `authentication_otp` is login-gating and carries a secret
> code — keep it on the Meta Cloud API. Only the other 10 sends route here.

---

## 0. TL;DR checklist

**On the WhatsApp Platform server (this repo):**
- [ ] `pip install -r requirements.txt` (adds `pywebpush`)
- [ ] `python generate_vapid_keys.py` → put `VAPID_PUBLIC_KEY` / `VAPID_PRIVATE_KEY` in `.env`
- [ ] Generate `WA_API_KEY`: `python -c "import secrets; print(secrets.token_urlsafe(32))"` → `.env`
- [ ] Restart the server (auto-migrates `conversations.campaign` + `push_subscriptions` tables)
- [ ] Smoke-test `/api/whatsapp/send` with curl (see §5)

**On OneHub (Supabase):**
- [ ] Add secrets `WA_API_URL`, `WA_API_KEY`, `USE_WA_API=true`
- [ ] Add `supabase/functions/_shared/whatsapp.ts` (see §4)
- [ ] Replace the local `fetch` in each of the **10** edge functions with the helper
- [ ] Leave `send-otp-whatsapp` on the direct Cloud API
- [ ] Deploy functions, send one of each template, verify in the inbox
- [ ] Fix the logging quirks in §6 while you're in there

Rollback at any time: set `USE_WA_API=false` on OneHub → every send falls back to
the direct Cloud API.

---

## 1. The endpoint

```
POST {WA_API_URL}/api/whatsapp/send
Authorization: Bearer {WA_API_KEY}
Content-Type: application/json

{
  "to": "+919812345678",                 // required; any format, normalised to digits
  "template": "onehub_webinar_confirmation", // required; approved template name
  "language": "en",                      // optional, default "en"
  "params": ["Asha", "MS Fabric 101", "https://ifsjaipur.com/webinars/..."],
                                         // ordered body vars {{1}}, {{2}}, {{3}} ...
  "buttons": [                           // optional; only authentication_otp needs this
    { "type": "url", "index": 0, "param": "483920" }
  ],
  "context": { "source": "confirm-webinar-payment", "user_id": "..." }, // logged only
  "name": "Asha",                        // optional; improves the CRM contact name
  "ai_reply": true                        // optional, default true. false = the reply
                                         //   goes to the human inbox, AI does not auto-reply
}
```

**Success →** `200 { "message_id": "wamid....", "status": "sent" }`
**Failure →** `4xx/5xx { "detail": "..." }` (e.g. `502` if Meta rejects the template)

Keep logging `message_id` to OneHub's `whatsapp_messages` exactly as today.

### Notes
- `params` is a **flat array of values** in `{{1}}, {{2}}, ...` order. The platform
  converts it to Meta's `components` for you. You do **not** build `components`.
- `buttons` builds a button component (`sub_type` = `type`, default `url`). Only
  `authentication_otp` uses it — and that one stays direct, so in practice you can omit it.
- The platform stores the template name as the conversation's `campaign` and feeds
  it to the AI, so replies are answered in context. Set `ai_reply: false` for
  admin-facing alerts and pure one-way blasts.

---

## 2. Auth

OneHub authenticates with a **dedicated service key**, separate from the human
dashboard password:

```
Authorization: Bearer {WA_API_KEY}
```

Set `WA_API_KEY` in the platform's `.env` and as a Supabase secret on OneHub.
Rotating it later doesn't affect the dashboard login.

---

## 3. Per-template cutover map (10 routed + 1 stays direct)

| # | Template | `params` (order) | `ai_reply` | OneHub function |
|---|----------|------------------|-----------|-----------------|
| 1 | `authentication_otp` | — | **STAYS ON DIRECT CLOUD API** | `send-otp-whatsapp` |
| 2 | `onehub_student_onboarding` | `[firstName, course]` | true | `onboard-student` |
| 3 | `onehub_student_referral` | `[referrerName]` | true | `send-referral-whatsapp` |
| 4 | `onehub_referral_success` | `[referrer, referred, course]` | true | `notify-referrer` |
| 5 | `birthday_wishes_students` | `[firstName]` | true | `send-birthday-wishes` |
| 6 | `onehub_training_request_alert` | `[org, contact, topics]` | **false** (admin recipient) | `notify-training-request` |
| 7 | `onehub_webinar_confirmation` | `[name, webinar, portalUrl]` | true | `confirm-webinar-payment` |
| 8 | `onehub_webinar_session_reminder` | `[name, webinar, portalUrl]` | true | `webinar-session-reminder` |
| 9 | `onehub_webinar_payment_pending` | `[name, webinar]` | true | `webinar-payment-reminder` |
| 10 | `onehub_webinar_broadcast` | `[name, webinar, message]` | true | `webinar-broadcast` (loop per recipient) |
| 11 | dynamic (broadcast page) | caller-supplied values | true | `send-whatsapp-broadcast` |

### Ready-to-use request bodies

**#2 onboarding**
```json
{ "to": "+91...", "template": "onehub_student_onboarding",
  "params": ["Asha", "MS Fabric 101"], "name": "Asha",
  "context": { "source": "onboard-student", "user_id": "..." } }
```

**#5 birthday**
```json
{ "to": "+91...", "template": "birthday_wishes_students",
  "params": ["Asha"], "name": "Asha",
  "context": { "source": "send-birthday-wishes", "user_id": "..." } }
```

**#6 training request alert (to admin — no AI reply)**
```json
{ "to": "+919829393505", "template": "onehub_training_request_alert",
  "params": ["Acme Corp", "Rahul (98xxxx)", "Power BI, Fabric"],
  "ai_reply": false,
  "context": { "source": "notify-training-request" } }
```

**#7 webinar confirmation**
```json
{ "to": "+91...", "template": "onehub_webinar_confirmation",
  "params": ["Asha", "MS Fabric 101", "https://ifsjaipur.com/webinars/abc"],
  "name": "Asha",
  "context": { "source": "confirm-webinar-payment", "user_id": "..." } }
```

**#10 webinar broadcast (call once per attendee in the existing loop)**
```json
{ "to": "+91...", "template": "onehub_webinar_broadcast",
  "params": ["Asha", "MS Fabric 101", "Reminder: session moved to 6 PM."],
  "name": "Asha",
  "context": { "source": "webinar-broadcast" } }
```

**#11 dynamic broadcast** — pass the admin-chosen template name and the per-recipient
values straight through as `params`.

**#1 OTP — DO NOT route. Keep the existing direct Cloud API call** in `send-otp-whatsapp`.

---

## 4. Shared helper for OneHub (`supabase/functions/_shared/whatsapp.ts`)

Single helper for all 10 routed call sites, with a **fallback flag** for safe rollout.

```ts
// supabase/functions/_shared/whatsapp.ts
const WA_API_URL = Deno.env.get("WA_API_URL");          // e.g. https://wa.ifsjaipur.com
const WA_API_KEY = Deno.env.get("WA_API_KEY");
const USE_WA_API = (Deno.env.get("USE_WA_API") ?? "true") === "true";

// Fallback (direct Cloud API) — same secrets you use today
const WHATSAPP_ACCESS_TOKEN = Deno.env.get("WHATSAPP_ACCESS_TOKEN")!;
const WHATSAPP_PHONE_NUMBER_ID = Deno.env.get("WHATSAPP_PHONE_NUMBER_ID")!;

export interface SendOpts {
  language?: string;
  buttons?: { type: string; index: number; param: string }[];
  context?: Record<string, unknown>;
  name?: string;
  aiReply?: boolean;
}

/** Send an approved WhatsApp template. Returns the Meta message id (wamid). */
export async function sendWhatsAppTemplate(
  to: string,
  template: string,
  params: (string | number)[] = [],
  opts: SendOpts = {},
): Promise<{ messageId: string }> {
  const phone = to.replace(/\D/g, ""); // digits only

  if (USE_WA_API && WA_API_URL && WA_API_KEY) {
    const res = await fetch(`${WA_API_URL}/api/whatsapp/send`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${WA_API_KEY}`,
      },
      body: JSON.stringify({
        to: phone,
        template,
        language: opts.language ?? "en",
        params,
        buttons: opts.buttons,
        context: opts.context,
        name: opts.name,
        ai_reply: opts.aiReply ?? true,
      }),
    });
    if (!res.ok) throw new Error(`WA API ${res.status}: ${await res.text()}`);
    const data = await res.json();
    return { messageId: data.message_id };
  }

  return sendDirectCloudApi(phone, template, params, opts);
}

/** Fallback: original direct Meta Cloud API path (used when USE_WA_API=false). */
async function sendDirectCloudApi(
  phone: string,
  template: string,
  params: (string | number)[],
  opts: SendOpts,
): Promise<{ messageId: string }> {
  const components: unknown[] = [];
  if (params.length) {
    components.push({
      type: "body",
      parameters: params.map((p) => ({ type: "text", text: String(p) })),
    });
  }
  for (const b of opts.buttons ?? []) {
    components.push({
      type: "button",
      sub_type: b.type ?? "url",
      index: String(b.index ?? 0),
      parameters: [{ type: "text", text: String(b.param) }],
    });
  }

  const res = await fetch(
    `https://graph.facebook.com/v21.0/${WHATSAPP_PHONE_NUMBER_ID}/messages`,
    {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${WHATSAPP_ACCESS_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        messaging_product: "whatsapp",
        recipient_type: "individual",
        to: phone,
        type: "template",
        template: {
          name: template,
          language: { code: opts.language ?? "en" },
          ...(components.length ? { components } : {}),
        },
      }),
    },
  );
  if (!res.ok) throw new Error(`Cloud API ${res.status}: ${await res.text()}`);
  const data = await res.json();
  return { messageId: data.messages?.[0]?.id ?? "" };
}
```

### Replacing a call site (example: `confirm-webinar-payment`)

```ts
import { sendWhatsAppTemplate } from "../_shared/whatsapp.ts";

const { messageId } = await sendWhatsAppTemplate(
  registrant.phone,
  "onehub_webinar_confirmation",
  [registrant.name, webinar.title, portalUrl],
  { name: registrant.name, context: { source: "confirm-webinar-payment", user_id: registrant.user_id } },
);
// then log messageId to whatsapp_messages as before
```

Do the same for the other 9. For `send-otp-whatsapp`, **leave the existing direct
fetch in place.**

---

## 5. Verify (curl)

```bash
curl -i -X POST "$WA_API_URL/api/whatsapp/send" \
  -H "Authorization: Bearer $WA_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{ "to": "+9198XXXXXXXX", "template": "birthday_wishes_students", "params": ["Test"], "name": "Test" }'
```

Expect `200 {"message_id":"wamid...","status":"sent"}` and the WhatsApp arriving on the
test phone. Then reply from that phone and confirm the thread (with the original
template preview) shows in the dashboard **Inbox**, tagged `(ERP)`, and that the AI
answers in context.

Bad key → `401`. Bad/unapproved template → `502` with Meta's error in `detail`.

---

## 6. Known quirks to fix during cutover (OneHub side)

From the OneHub message inventory — clean these up while replacing the call sites:

- **`onboard-student` logs the wrong name:** it writes `template_name: "onboarding_notification"`
  but actually sends `onehub_student_onboarding`. Log the real template.
- **6 of 11 sends don't write a `whatsapp_messages` row** (webinar + training + referrer
  notifications). Add logging so every send is tracked by `message_id`.
- **Inconsistent id column:** most rows use `whatsapp_message_id`, `onboard-student`
  uses `wa_message_id`. Standardise on one.
- **`webinar-payment-reminder` cron is not in repo migrations** (scheduled in the
  Supabase dashboard). Confirm it still fires after redeploy.

---

## 7. What the platform does on its side (FYI)

- Sends the template via the Cloud API and returns the `wamid`.
- Records a readable preview into the contact's conversation (`source="erp"`,
  shown as `(ERP)` in the inbox).
- Stores the template name as the conversation `campaign`; the chatbot uses it so
  replies are answered in context instead of the generic fallback.
- `ai_reply:false` flips the contact's AI off so the reply waits for a human in the
  inbox (re-enable anytime with the inbox "Resume AI" button).
- Fires an Android push notification (if VAPID + a subscribed device are configured).

No schema work needed on OneHub. The platform auto-migrates its own tables on restart.
