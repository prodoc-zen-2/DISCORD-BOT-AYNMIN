# Scaling Guide

## 1) Duplicate this bot for another sheet
1. Copy the project folder.
2. Set a different `.env` per bot instance.
3. Change these values in the copied `.env`:
- `GOOGLE_SHEET_ID`
- `GOOGLE_WORKSHEET_NAME`
- `LIVE_SYNC_CHANNEL_ID`
- `DUE_SOON_CHANNEL_ID`
- `OWNER_ID_MAP` and `OWNER_MENTION_ORDER`
4. Run each bot with its own Discord token if you want separate bots.

## 2) Keep one bot, multiple sheets (recommended pattern)
Add a config block per team/sheet in code (or JSON), each with:
- sheet id
- worksheet name
- live channel id
- reminder channel id
- owner mapping

Then instantiate one `SheetsTodoService` per block and run a sync/reminder loop per block.

## 3) Required Google Sheet columns
The bot auto-maps by header aliases, but keep these headers for best stability:
- `Task`
- `Owner`
- `Status`
- `Deadline`
- `Deliverable`
- `Notes`

Allowed status values:
- `Not started`
- `In progress`
- `Blocked`
- `Completed`

## 4) Reminder ordering and mentions
- Mention mapping priority:
1. `OWNER_ID_MAP`
2. `OWNER_1_NAME/ID` ... `OWNER_4_NAME/ID`
3. Discord member name lookup
4. fallback text mention

- Reminder owner order:
Use `OWNER_MENTION_ORDER` (comma-separated names). Owners not listed are appended alphabetically.

## 5) Recommended ops checklist
1. Keep `SYNC_POLL_SECONDS` >= 10.
2. Keep `DUE_SOON_INTERVAL_HOURS` >= 4 for rate safety.
3. Use `!testannounce` after any channel or role permission change.
4. Ensure bot has `View Channel`, `Send Messages`, `Read Message History` in both live/reminder channels.
