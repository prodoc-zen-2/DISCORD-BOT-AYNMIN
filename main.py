import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
import base64

import discord
import gspread
from discord.ext import commands
from dotenv import load_dotenv


load_dotenv()

# Initialize service account from base64 env var if deployment setting exists
def initialize_service_account():
    """Decode base64 service account JSON from env var and write to file."""
    sa_b64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_B64")
    sa_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "service_account.json")
    
    # If base64 env var exists, decode and write it
    if sa_b64:
        try:
            sa_json = base64.b64decode(sa_b64).decode('utf-8')
            with open(sa_file, 'w', encoding='utf-8') as f:
                f.write(sa_json)
            logger_temp = logging.getLogger(__name__)
            logger_temp.info(f"Service account initialized from GOOGLE_SERVICE_ACCOUNT_JSON_B64 env var")
        except Exception as exc:
            logger_temp = logging.getLogger(__name__)
            logger_temp.error(f"Failed to initialize service account from env var: {exc}")
    elif not os.path.exists(sa_file):
        logger_temp = logging.getLogger(__name__)
        logger_temp.warning(f"Service account file '{sa_file}' not found and GOOGLE_SERVICE_ACCOUNT_JSON_B64 not set")


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("discord.log"),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

# Initialize service account before anything else
initialize_service_account()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "service_account.json")

ALLOWED_STATUSES = ["Not started", "In progress", "Blocked", "Completed"]
STATUS_ALIASES = {
    "not started": "Not started",
    "not_started": "Not started",
    "todo": "Not started",
    "in progress": "In progress",
    "in_progress": "In progress",
    "doing": "In progress",
    "blocked": "Blocked",
    "done": "Completed",
    "complete": "Completed",
    "completed": "Completed",
    "true": "Completed",
    "false": "Not started",
}


def parse_positive_int(var_name: str, default: int) -> int:
    raw = (os.getenv(var_name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
        return value if value > 0 else default
    except ValueError:
        return default


SYNC_POLL_SECONDS = parse_positive_int("SYNC_POLL_SECONDS", 15)
DUE_SOON_INTERVAL_HOURS = parse_positive_int("DUE_SOON_INTERVAL_HOURS", 4)
DUE_SOON_DAYS = parse_positive_int("DUE_SOON_DAYS", 3)


def parse_channel_id(raw: str | None):
    value = (raw or "").strip()
    return int(value) if value.isdigit() else None


def safe_positive_int(value, fallback: int):
    try:
        n = int(value)
        return n if n > 0 else fallback
    except (TypeError, ValueError):
        return fallback


def resolve_env_reference(value):
    if not isinstance(value, str):
        return value
    raw = value.strip()
    if raw.startswith("${") and raw.endswith("}"):
        env_key = raw[2:-1].strip()
        return os.getenv(env_key, "")
    return value


def load_boards_file():
    try:
        with open("bot_boards.json", "r", encoding="utf-8") as fp:
            return json.load(fp)
    except FileNotFoundError:
        logger.warning("bot_boards.json not found, returning empty board list")
        return {"boards": []}
    except json.JSONDecodeError as exc:
        logger.error(f"Failed to parse bot_boards.json: {exc}")
        return {"boards": []}


def save_active_board_id(board_id: str):
    try:
        data = load_boards_file()
        data["activeBoard"] = board_id
        with open("bot_boards.json", "w", encoding="utf-8") as fp:
            json.dump(data, fp, indent=2)
    except OSError as exc:
        logger.error(f"Failed to save active board ID to bot_boards.json: {exc}")
    except json.JSONDecodeError as exc:
        logger.error(f"Failed to encode board data as JSON: {exc}")


def load_runtime_board_config(selected_board_id: str | None = None):
    config = {
        "boardId": None,
        "boardName": "",
        "boardFound": False,
        "googleSheetId": os.getenv("GOOGLE_SHEET_ID"),
        "worksheetName": os.getenv("GOOGLE_WORKSHEET_NAME", "Sheet1"),
        "todoBoardName": "TASK BOARD",
        "liveSyncChannelId": os.getenv("LIVE_SYNC_CHANNEL_ID"),
        "dueSoonChannelId": os.getenv("DUE_SOON_CHANNEL_ID"),
        "syncPollSeconds": parse_positive_int("SYNC_POLL_SECONDS", 15),
        "dueSoonIntervalHours": parse_positive_int("DUE_SOON_INTERVAL_HOURS", 4),
        "dueSoonDays": parse_positive_int("DUE_SOON_DAYS", 3),
        "ownerIdMap": {},
        "ownerMentionOrder": [],
    }

    try:
        data = load_boards_file()
        active_id = selected_board_id or os.getenv("ACTIVE_BOARD_ID") or data.get("activeBoard")
        config["boardId"] = active_id
        board = None
        for item in data.get("boards", []):
            if item.get("id") == active_id:
                board = item
                break

        if board:
            config["boardFound"] = True
            config["boardId"] = board.get("id")
            config["boardName"] = board.get("name", "")
            config["googleSheetId"] = resolve_env_reference(board.get("googleSheetId")) or config["googleSheetId"]
            config["worksheetName"] = resolve_env_reference(board.get("worksheetName")) or config["worksheetName"] or "Sheet1"
            config["todoBoardName"] = resolve_env_reference(board.get("todoBoardName")) or "TASK BOARD"
            config["liveSyncChannelId"] = resolve_env_reference(board.get("liveSyncChannelId")) or config["liveSyncChannelId"]
            config["dueSoonChannelId"] = resolve_env_reference(board.get("dueSoonChannelId")) or config["dueSoonChannelId"]
            config["syncPollSeconds"] = safe_positive_int(resolve_env_reference(board.get("syncPollSeconds")), config["syncPollSeconds"])
            config["dueSoonIntervalHours"] = safe_positive_int(
                resolve_env_reference(board.get("dueSoonIntervalHours")), config["dueSoonIntervalHours"]
            )
            config["dueSoonDays"] = safe_positive_int(resolve_env_reference(board.get("dueSoonDays")), config["dueSoonDays"])
            if isinstance(board.get("ownerIdMap"), dict):
                config["ownerIdMap"] = {str(k).lower(): str(v) for k, v in board.get("ownerIdMap", {}).items() if str(v).isdigit()}
            if isinstance(board.get("ownerMentionOrder"), list):
                config["ownerMentionOrder"] = [str(v).strip().lower() for v in board.get("ownerMentionOrder", []) if str(v).strip()]
    except (FileNotFoundError, json.JSONDecodeError, KeyError, ValueError) as exc:
        logger.warning(f"Error loading board config for {selected_board_id}: {exc}")

    return config


RUNTIME_CONFIG = load_runtime_board_config()
ACTIVE_BOARD_ID = RUNTIME_CONFIG.get("boardId")
GOOGLE_SHEET_ID = RUNTIME_CONFIG.get("googleSheetId")
GOOGLE_WORKSHEET_NAME = RUNTIME_CONFIG.get("worksheetName", "Sheet1")
TODO_BOARD_NAME = RUNTIME_CONFIG.get("todoBoardName", "TASK BOARD")
LIVE_SYNC_CHANNEL_INT = parse_channel_id(RUNTIME_CONFIG.get("liveSyncChannelId"))
DUE_SOON_CHANNEL_INT = parse_channel_id(RUNTIME_CONFIG.get("dueSoonChannelId"))
SYNC_POLL_SECONDS = RUNTIME_CONFIG.get("syncPollSeconds", SYNC_POLL_SECONDS)
DUE_SOON_INTERVAL_HOURS = RUNTIME_CONFIG.get("dueSoonIntervalHours", DUE_SOON_INTERVAL_HOURS)
DUE_SOON_DAYS = RUNTIME_CONFIG.get("dueSoonDays", DUE_SOON_DAYS)


def list_boards_from_file():
    data = load_boards_file()
    boards = data.get("boards", [])
    active_id = os.getenv("ACTIVE_BOARD_ID") or data.get("activeBoard")
    return boards, active_id


handler = logging.FileHandler(filename="discord.log", encoding="utf-8", mode="w")
intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)


@dataclass
class TaskItem:
    row: int
    title: str
    owner: str
    status: str
    deadline: str
    deliverable: str
    notes: str

    @property
    def done(self) -> bool:
        return normalize_status(self.status) == "Completed"


class SheetsTodoService:
    def __init__(self, credentials_file: str, sheet_id: str, worksheet_name: str):
        self._credentials_file = credentials_file
        self._sheet_id = sheet_id
        self._worksheet_name = worksheet_name
        self._worksheet = None

    def _connect(self):
        gc = gspread.service_account(filename=self._credentials_file)
        spreadsheet = gc.open_by_key(self._sheet_id)
        self._worksheet = spreadsheet.worksheet(self._worksheet_name)

    def _ensure_connection(self):
        if self._worksheet is None:
            self._connect()

    def _get_rows(self):
        self._ensure_connection()
        return self._worksheet.get_all_values()

    @staticmethod
    def _header_map(rows):
        if not rows:
            return {}
        mapped = {}
        for idx, name in enumerate(rows[0]):
            key = str(name).strip().lower()
            if key:
                mapped[key] = idx
        return mapped

    @staticmethod
    def _find_column(header_map, aliases, default_index):
        for alias in aliases:
            if alias in header_map:
                return header_map[alias]
        return default_index

    def _get_column_indices(self, rows):
        headers = self._header_map(rows)
        return {
            "task": self._find_column(headers, ["task", "tasks", "todo", "to do"], 0),
            "owner": self._find_column(headers, ["owner", "assignee", "assigned to"], 1),
            "status": self._find_column(headers, ["status", "done", "checked"], 2),
            "deadline": self._find_column(headers, ["deadline", "due", "due date"], 3),
            "deliverable": self._find_column(headers, ["deliverable", "file", "link"], 4),
            "notes": self._find_column(headers, ["notes", "note", "remarks"], 5),
        }

    def list_tasks(self):
        rows = self._get_rows()
        if not rows:
            return []

        cols = self._get_column_indices(rows)
        start_row = 2
        tasks = []

        for row_index in range(start_row - 1, len(rows)):
            row = rows[row_index]
            title = row[cols["task"]].strip() if len(row) > cols["task"] else ""
            if not title or title.lower() == "task":
                continue

            owner = row[cols["owner"]].strip() if len(row) > cols["owner"] else ""
            status_raw = row[cols["status"]].strip() if len(row) > cols["status"] else ""
            deadline = row[cols["deadline"]].strip() if len(row) > cols["deadline"] else ""
            deliverable = row[cols["deliverable"]].strip() if len(row) > cols["deliverable"] else ""
            notes = row[cols["notes"]].strip() if len(row) > cols["notes"] else ""

            tasks.append(
                TaskItem(
                    row=row_index + 1,
                    title=title,
                    owner=owner,
                    status=normalize_status(status_raw),
                    deadline=deadline,
                    deliverable=deliverable,
                    notes=notes,
                )
            )

        return tasks

    def _build_row_payload(self, rows, title, owner, status, deadline, deliverable, notes):
        cols = self._get_column_indices(rows)
        row_length = max(len(rows[0]) if rows else 0, 6)
        payload = [""] * row_length

        for key, value in [
            ("task", title),
            ("owner", owner),
            ("status", status),
            ("deadline", deadline),
            ("deliverable", deliverable),
            ("notes", notes),
        ]:
            col_idx = cols[key]
            if col_idx >= len(payload):
                payload.extend([""] * (col_idx - len(payload) + 1))
            payload[col_idx] = value

        return payload

    def append_task(
        self,
        title: str,
        owner: str = "",
        status: str = "Not started",
        deadline: str = "",
        deliverable: str = "",
        notes: str = "",
    ):
        self._ensure_connection()
        rows = self._get_rows()
        normalized_status = parse_status_input(status)
        if normalized_status is None:
            raise ValueError("Status must be Not started, In progress, Blocked, or Completed")
        payload = self._build_row_payload(
            rows,
            title=title,
            owner=owner,
            status=normalized_status,
            deadline=deadline,
            deliverable=deliverable,
            notes=notes,
        )
        self._worksheet.append_row(payload, value_input_option="USER_ENTERED")

    def update_task(
        self,
        task_number: int,
        title: str | None = None,
        owner: str | None = None,
        status: str | None = None,
        deadline: str | None = None,
        deliverable: str | None = None,
        notes: str | None = None,
    ):
        tasks = self.list_tasks()
        if task_number < 1 or task_number > len(tasks):
            raise IndexError("Task number out of range")

        target = tasks[task_number - 1]
        rows = self._get_rows()
        cols = self._get_column_indices(rows)
        self._ensure_connection()

        updates = {
            "task": title,
            "owner": owner,
            "status": parse_status_input(status) if status is not None else None,
            "deadline": deadline,
            "deliverable": deliverable,
            "notes": notes,
        }

        if status is not None and updates["status"] is None:
            raise ValueError("Status must be Not started, In progress, Blocked, or Completed")

        for key, value in updates.items():
            if value is None:
                continue
            col = cols[key] + 1
            self._worksheet.update_cell(target.row, col, value)

    def delete_task(self, task_number: int):
        tasks = self.list_tasks()
        if task_number < 1 or task_number > len(tasks):
            raise IndexError("Task number out of range")

        target = tasks[task_number - 1]
        self._ensure_connection()
        self._worksheet.delete_rows(target.row)

    def set_task_status(self, task_number: int, status: str):
        normalized_status = parse_status_input(status)
        if normalized_status is None:
            raise ValueError("Status must be Not started, In progress, Blocked, or Completed")
        self.update_task(task_number=task_number, status=normalized_status)

    def toggle_task_completion(self, task_number: int):
        tasks = self.list_tasks()
        if task_number < 1 or task_number > len(tasks):
            raise IndexError("Task number out of range")

        target = tasks[task_number - 1]
        new_status = "Not started" if target.done else "Completed"
        self.set_task_status(task_number, new_status)


def normalize_status(raw: str | None) -> str:
    return parse_status_input(raw) or "Not started"


def parse_status_input(raw: str | None):
    value = (raw or "").strip().lower()
    if not value:
        return None
    return STATUS_ALIASES.get(value)


def parse_deadline(date_text: str):
    raw = (date_text or "").strip()
    if not raw:
        return None

    for fmt in ["%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%m-%d-%Y", "%m.%d.%Y"]:
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def status_emoji(status: str) -> str:
    normalized = normalize_status(status)
    return {
        "Not started": "NS",
        "In progress": "IP",
        "Blocked": "BLK",
        "Completed": "DONE",
    }.get(normalized, "NS")


def build_owner_id_map() -> dict[str, str]:
    mapping = dict(RUNTIME_CONFIG.get("ownerIdMap", {}))
    raw = (os.getenv("OWNER_ID_MAP") or "").strip()
    if raw:
        for pair in raw.split(","):
            if ":" not in pair:
                continue
            name, user_id = pair.split(":", 1)
            name = name.strip()
            user_id = user_id.strip()
            if name and user_id.isdigit():
                mapping[name.lower()] = user_id

    for i in range(1, 5):
        owner_name = (os.getenv(f"OWNER_{i}_NAME") or "").strip()
        owner_id = (os.getenv(f"OWNER_{i}_ID") or "").strip()
        if owner_name and owner_id.isdigit():
            mapping[owner_name.lower()] = owner_id

    return mapping


OWNER_ID_MAP = build_owner_id_map()
OWNER_MENTION_ORDER = [name.strip().lower() for name in (os.getenv("OWNER_MENTION_ORDER") or "").split(",") if name.strip()]
if not OWNER_MENTION_ORDER:
    OWNER_MENTION_ORDER = list(RUNTIME_CONFIG.get("ownerMentionOrder", []))

todo_service = None
sync_task = None
due_soon_task = None
live_snapshots = {}
live_message_ids = {}
due_soon_last_sent = {}
slash_commands_synced = False


def rebuild_owner_runtime():
    global OWNER_ID_MAP
    global OWNER_MENTION_ORDER
    OWNER_ID_MAP = build_owner_id_map()
    OWNER_MENTION_ORDER = [name.strip().lower() for name in (os.getenv("OWNER_MENTION_ORDER") or "").split(",") if name.strip()]
    if not OWNER_MENTION_ORDER:
        OWNER_MENTION_ORDER = list(RUNTIME_CONFIG.get("ownerMentionOrder", []))


def switch_active_board(board_id: str, persist: bool = True):
    global ACTIVE_BOARD_ID
    global RUNTIME_CONFIG
    global GOOGLE_SHEET_ID
    global GOOGLE_WORKSHEET_NAME
    global TODO_BOARD_NAME
    global LIVE_SYNC_CHANNEL_INT
    global DUE_SOON_CHANNEL_INT
    global SYNC_POLL_SECONDS
    global DUE_SOON_INTERVAL_HOURS
    global DUE_SOON_DAYS
    global todo_service
    global live_snapshots
    global live_message_ids

    config = load_runtime_board_config(board_id)
    if not config.get("boardFound"):
        return False, "Board ID not found in bot_boards.json"

    RUNTIME_CONFIG = config
    ACTIVE_BOARD_ID = config.get("boardId")
    GOOGLE_SHEET_ID = config.get("googleSheetId")
    GOOGLE_WORKSHEET_NAME = config.get("worksheetName", "Sheet1")
    TODO_BOARD_NAME = config.get("todoBoardName", "TASK BOARD")
    LIVE_SYNC_CHANNEL_INT = parse_channel_id(config.get("liveSyncChannelId"))
    DUE_SOON_CHANNEL_INT = parse_channel_id(config.get("dueSoonChannelId"))
    SYNC_POLL_SECONDS = config.get("syncPollSeconds", SYNC_POLL_SECONDS)
    DUE_SOON_INTERVAL_HOURS = config.get("dueSoonIntervalHours", DUE_SOON_INTERVAL_HOURS)
    DUE_SOON_DAYS = config.get("dueSoonDays", DUE_SOON_DAYS)

    rebuild_owner_runtime()

    if not GOOGLE_SHEET_ID:
        return False, "Selected board has no googleSheetId configured"

    todo_service = SheetsTodoService(
        credentials_file=GOOGLE_SERVICE_ACCOUNT_FILE,
        sheet_id=GOOGLE_SHEET_ID,
        worksheet_name=GOOGLE_WORKSHEET_NAME,
    )
    live_snapshots = {}
    live_message_ids = {}

    if persist:
        save_active_board_id(board_id)

    return True, None


def all_runtime_board_configs():
    data = load_boards_file()
    configs = []
    for board in data.get("boards", []):
        board_id = str(board.get("id", "")).strip()
        if not board_id:
            continue
        cfg = load_runtime_board_config(board_id)
        if cfg.get("boardFound") and cfg.get("googleSheetId"):
            configs.append(cfg)

    if not configs and GOOGLE_SHEET_ID:
        configs.append(RUNTIME_CONFIG)
    return configs


def create_service_for_config(config):
    return SheetsTodoService(
        credentials_file=GOOGLE_SERVICE_ACCOUNT_FILE,
        sheet_id=config.get("googleSheetId"),
        worksheet_name=config.get("worksheetName", "Sheet1"),
    )


async def get_channel_by_id(channel_id: int | None):
    if not channel_id:
        return None, "Channel ID is empty or invalid"

    channel = bot.get_channel(channel_id)
    if channel is not None:
        return channel, None

    try:
        fetched = await bot.fetch_channel(channel_id)
        return fetched, None
    except discord.NotFound:
        return None, "Channel ID was not found"
    except discord.Forbidden:
        return None, "Bot has no permission to view that channel"
    except discord.HTTPException as exc:
        return None, f"Discord API error while fetching channel: {exc}"
    except Exception as exc:
        return None, f"Unexpected error while fetching channel: {exc}"


def due_soon_tasks(tasks):
    now = datetime.now(UTC).date()
    result = []
    for task in tasks:
        if normalize_status(task.status) == "Completed":
            continue
        due = parse_deadline(task.deadline)
        if due is None:
            continue
        days_left = (due - now).days
        if 0 <= days_left <= DUE_SOON_DAYS:
            result.append((task, days_left))
    return result


def due_soon_tasks_for_display(tasks):
    now = datetime.now(UTC).date()
    result = []
    for task in tasks:
        due = parse_deadline(task.deadline)
        if due is None:
            continue
        days_left = (due - now).days
        if 0 <= days_left <= DUE_SOON_DAYS:
            result.append((task, days_left))
    return result


def format_deadline_for_ui(deadline: str):
    parsed = parse_deadline(deadline)
    if parsed is None:
        return deadline or "No deadline"
    return parsed.strftime("%b %d, %Y")

def deadline_state(task: TaskItem):
    parsed = parse_deadline(task.deadline)
    if parsed is None:
        return "No deadline"
    days_left = (parsed - datetime.now(UTC).date()).days
    if days_left < 0:
        return f"Late by {abs(days_left)} day(s)"
    if days_left == 0:
        return "Due today"
    if days_left <= DUE_SOON_DAYS:
        return f"Due in {days_left} day(s)"
    return f"Due {parsed.strftime('%b %d')}"

def status_icon(status: str):
    normalized = normalize_status(status)
    return {
        "Not started": "⬜",
        "In progress": "🟨",
        "Blocked": "⛔",
        "Completed": "✅",
    }.get(normalized, "⬜")


def build_dashboard_text(tasks, board_title: str | None = None):
    ordered = list(tasks)

    def due_text(task: TaskItem):
        parsed = parse_deadline(task.deadline)
        if parsed is None:
            return "Due: none"
        return f"Due: {parsed.strftime('%b %d, %Y')}"

    lines = [
        f"               {board_title or TODO_BOARD_NAME}",
        "",
        "Tasks",
    ]

    if ordered:
        for idx, task in enumerate(ordered[:25], start=1):
            lines.append(f"{idx}. {status_icon(task.status)} {task.title}")
            lines.append(f"   Owner: {task.owner or 'Unassigned'} | Status: {normalize_status(task.status)} | 📅 {due_text(task)}")
            lines.append("")
    else:
        lines.append("No tasks found in Google Sheets.")

    return "\n".join(lines)


def build_tasks_embed(tasks, title: str = "Task Control Center"):
    status_counts = {status: 0 for status in ALLOWED_STATUSES}
    for task in tasks:
        status_counts[normalize_status(task.status)] = status_counts.get(normalize_status(task.status), 0) + 1

    embed = discord.Embed(
        title=title,
        description=(
            "Interactive board synced with Google Sheets. "
            "Use the dropdown to quick-toggle completion and buttons to create, edit, or delete tasks."
        ),
        color=discord.Color.from_rgb(32, 137, 220),
        timestamp=datetime.now(UTC),
    )
    embed.add_field(
        name="Summary",
        value=(
            f"Not started: **{status_counts.get('Not started', 0)}**\n"
            f"In progress: **{status_counts.get('In progress', 0)}**\n"
            f"Blocked: **{status_counts.get('Blocked', 0)}**\n"
            f"Completed: **{status_counts.get('Completed', 0)}**"
        ),
        inline=True,
    )

    due_soon = due_soon_tasks(tasks)
    embed.add_field(
        name=f"Due in {DUE_SOON_DAYS} Days",
        value=str(len(due_soon)),
        inline=True,
    )

    if tasks:
        lines = []
        for i, task in enumerate(tasks[:10], start=1):
            emoji = status_emoji(task.status)
            owner = task.owner if task.owner else "Unassigned"
            due = task.deadline if task.deadline else "No deadline"
            lines.append(f"**{i}.** {emoji} {task.title}\nOwner: {owner} | Due: {due}")
        embed.add_field(name="Top Tasks", value="\n\n".join(lines), inline=False)
    else:
        embed.add_field(name="Top Tasks", value="No tasks found in Google Sheets.", inline=False)

    embed.set_footer(text="Statuses: Not started | In progress | Blocked | Completed")
    return embed


async def resolve_owner_mention(owner_value: str, guild: discord.Guild | None):
    owner = (owner_value or "").strip()
    if not owner:
        return None

    if owner.startswith("<@") and owner.endswith(">"):
        return owner
    if owner.isdigit():
        return f"<@{owner}>"

    mapped_id = OWNER_ID_MAP.get(owner.lower())
    if mapped_id:
        return f"<@{mapped_id}>"

    if guild:
        normalized = owner.lower()
        for member in guild.members:
            if member.display_name.lower() == normalized or member.name.lower() == normalized:
                return member.mention

    return f"@{owner}"


class CreateTaskModal(discord.ui.Modal, title="Create Task"):
    task = discord.ui.TextInput(label="Task", placeholder="Task title", required=True, max_length=120)
    owner = discord.ui.TextInput(label="Owner", placeholder="Name from sheet or Discord ID", required=False, max_length=60)
    status = discord.ui.TextInput(
        label="Status",
        placeholder="Not started / In progress / Blocked / Completed",
        required=False,
        max_length=30,
        default="Not started",
    )
    deadline = discord.ui.TextInput(label="Deadline", placeholder="m/d/yyyy", required=False, max_length=20)
    notes = discord.ui.TextInput(label="Notes", placeholder="Optional notes", required=False, style=discord.TextStyle.paragraph)

    def __init__(self, board_id: str):
        super().__init__()
        self._board_id = board_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await defer_interaction(interaction)
            service, cfg = get_board_service_and_config(self._board_id)
            raw_status = str(self.status).strip() or "Not started"
            normalized_status = parse_status_input(raw_status)
            if normalized_status is None:
                await interaction.followup.send(
                    "Status must be one of: Not started, In progress, Blocked, Completed",
                    ephemeral=True,
                )
                return
            await asyncio.to_thread(
                service.append_task,
                title=str(self.task),
                owner=str(self.owner),
                status=normalized_status,
                deadline=str(self.deadline),
                notes=str(self.notes),
            )
            tasks = await asyncio.to_thread(service.list_tasks)
            board_title = cfg.get("todoBoardName") or cfg.get("boardName") or TODO_BOARD_NAME
            await edit_interaction_message(
                interaction,
                content=build_dashboard_text(tasks, board_title=board_title),
                view=TaskControlView(tasks, self._board_id),
                embed=None,
            )
        except Exception as exc:
            await interaction.followup.send(f"Failed to create task: {exc}", ephemeral=True)


def get_board_service_and_config(board_id: str):
    cfg = load_runtime_board_config(board_id)
    if not cfg.get("boardFound"):
        cfg = RUNTIME_CONFIG
    service = create_service_for_config(cfg)
    return service, cfg


async def defer_interaction(interaction: discord.Interaction):
    if interaction.response.is_done():
        return
    try:
        await interaction.response.defer()
    except discord.NotFound:
        logger.warning("Interaction already responded")
    except discord.InteractionResponded:
        logger.warning("Interaction already responded")


async def edit_interaction_message(interaction: discord.Interaction, *, content: str, view=None, embed=None):
    if interaction.message is not None:
        await interaction.message.edit(content=content, view=view, embed=embed)
        return
    await interaction.edit_original_response(content=content, view=view, embed=embed)


class EditTaskModal(discord.ui.Modal, title="Edit Task"):
    title_field = discord.ui.TextInput(label="Task (optional)", required=False, max_length=120)
    owner = discord.ui.TextInput(label="Owner (optional)", required=False, max_length=60)
    deadline = discord.ui.TextInput(label="Deadline (optional)", placeholder="m/d/yyyy", required=False, max_length=20)

    def __init__(self, task_number: int, board_id: str, task: TaskItem | None = None):
        super().__init__()
        self._task_number = task_number
        self._board_id = board_id
        if task is not None:
            self.title_field.default = task.title
            self.owner.default = task.owner
            self.deadline.default = task.deadline

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await defer_interaction(interaction)
            service, cfg = get_board_service_and_config(self._board_id)
            title_value = str(self.title_field).strip()
            owner_value = str(self.owner).strip()
            deadline_value = str(self.deadline).strip()

            await asyncio.to_thread(
                service.update_task,
                task_number=self._task_number,
                title=title_value if title_value else None,
                owner=owner_value if owner_value else None,
                deadline=deadline_value if deadline_value else None,
            )
            tasks = await asyncio.to_thread(service.list_tasks)
            board_title = cfg.get("todoBoardName") or cfg.get("boardName") or TODO_BOARD_NAME
            await edit_interaction_message(
                interaction,
                content=build_dashboard_text(tasks, board_title=board_title),
                view=TaskControlView(tasks, self._board_id),
                embed=None,
            )
        except Exception as exc:
            await interaction.followup.send(f"Failed to edit task: {exc}", ephemeral=True)


class StatusPickSelect(discord.ui.Select):
    def __init__(self, task_number: int, current_status: str, board_id: str):
        self._task_number = task_number
        self._board_id = board_id
        options = [
            discord.SelectOption(label="Not started", emoji="⬜", value="Not started", default=current_status == "Not started"),
            discord.SelectOption(label="In progress", emoji="🟨", value="In progress", default=current_status == "In progress"),
            discord.SelectOption(label="Blocked", emoji="⛔", value="Blocked", default=current_status == "Blocked"),
            discord.SelectOption(label="Completed", emoji="✅", value="Completed", default=current_status == "Completed"),
        ]
        super().__init__(placeholder="Pick a status...", min_values=1, max_values=1, options=options, row=0)

    async def callback(self, interaction: discord.Interaction):
        try:
            await defer_interaction(interaction)
            service, cfg = get_board_service_and_config(self._board_id)
            chosen_status = self.values[0]
            await asyncio.to_thread(service.set_task_status, self._task_number, chosen_status)
            tasks = await asyncio.to_thread(service.list_tasks)
            board_title = cfg.get("todoBoardName") or cfg.get("boardName") or TODO_BOARD_NAME
            await edit_interaction_message(
                interaction,
                content=build_dashboard_text(tasks, board_title=board_title),
                view=TaskControlView(tasks, self._board_id),
                embed=None,
            )
        except Exception as exc:
            await interaction.followup.send(f"Failed to set status: {exc}", ephemeral=True)


class SetTaskStatusModal(discord.ui.Modal, title="Set Task Status"):
    status = discord.ui.TextInput(
        label="Status",
        placeholder="Not started / In progress / Blocked / Completed",
        required=True,
        max_length=30,
    )

    def __init__(self, task_number: int, board_id: str, current_status: str):
        super().__init__()
        self._task_number = task_number
        self._board_id = board_id
        self.status.default = current_status

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await defer_interaction(interaction)
            service, cfg = get_board_service_and_config(self._board_id)
            raw_status = str(self.status).strip()
            normalized_status = parse_status_input(raw_status)
            if normalized_status is None:
                await interaction.followup.send(
                    "Status must be one of: Not started, In progress, Blocked, Completed",
                    ephemeral=True,
                )
                return

            await asyncio.to_thread(service.set_task_status, self._task_number, normalized_status)
            tasks = await asyncio.to_thread(service.list_tasks)
            board_title = cfg.get("todoBoardName") or cfg.get("boardName") or TODO_BOARD_NAME
            await edit_interaction_message(
                interaction,
                content=build_dashboard_text(tasks, board_title=board_title),
                view=TaskControlView(tasks, self._board_id),
                embed=None,
            )
        except Exception as exc:
            await interaction.followup.send(f"Failed to set status: {exc}", ephemeral=True)


class DeleteTaskModal(discord.ui.Modal, title="Delete Task"):
    task_number = discord.ui.TextInput(label="Task Number", placeholder="1", required=True, max_length=4)

    def __init__(self, board_id: str):
        super().__init__()
        self._board_id = board_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await defer_interaction(interaction)
            service, cfg = get_board_service_and_config(self._board_id)
            number = int(str(self.task_number).strip())
            await asyncio.to_thread(service.delete_task, number)
            tasks = await asyncio.to_thread(service.list_tasks)
            board_title = cfg.get("todoBoardName") or cfg.get("boardName") or TODO_BOARD_NAME
            await edit_interaction_message(
                interaction,
                content=build_dashboard_text(tasks, board_title=board_title),
                view=TaskControlView(tasks, self._board_id),
                embed=None,
            )
        except ValueError:
            await interaction.followup.send("Task number must be a number.", ephemeral=True)
        except Exception as exc:
            await interaction.followup.send(f"Failed to delete task: {exc}", ephemeral=True)


class TaskControlView(discord.ui.View):
    def __init__(self, tasks, board_id: str):
        super().__init__(timeout=None)
        self._board_id = board_id
        if tasks:
            self.add_item(TaskStatusSelect(tasks[:25], board_id))

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, row=2)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await defer_interaction(interaction)
        service, cfg = get_board_service_and_config(self._board_id)
        refreshed = await asyncio.to_thread(service.list_tasks)
        board_title = cfg.get("todoBoardName") or cfg.get("boardName") or TODO_BOARD_NAME
        await edit_interaction_message(
            interaction,
            content=build_dashboard_text(refreshed, board_title=board_title),
            view=TaskControlView(refreshed, self._board_id),
            embed=None,
        )


class TaskStatusSelect(discord.ui.Select):
    def __init__(self, tasks, board_id: str):
        self._board_id = board_id
        options = []
        for idx, task in enumerate(tasks[:25], start=1):
            options.append(
                discord.SelectOption(
                    label=f"{idx}. {task.title[:80]}",
                    description=f"{normalize_status(task.status)} | {task.owner or 'Unassigned'}",
                    value=str(idx),
                )
            )
        super().__init__(
            placeholder="Select task to set status...",
            min_values=1,
            max_values=1,
            options=options,
            row=0,
        )

    async def callback(self, interaction: discord.Interaction):
        try:
            service, _cfg = get_board_service_and_config(self._board_id)
            selected_idx = int(self.values[0])
            tasks = await asyncio.to_thread(service.list_tasks)
            if selected_idx < 1 or selected_idx > len(tasks):
                await interaction.response.send_message("Selected task is out of range.", ephemeral=True)
                return
            chosen = tasks[selected_idx - 1]
            current_status = normalize_status(chosen.status)
            await interaction.response.send_modal(SetTaskStatusModal(selected_idx, self._board_id, current_status))
        except Exception as exc:
            if interaction.response.is_done():
                await interaction.followup.send(f"Failed to open status modal: {exc}", ephemeral=True)
            else:
                await interaction.response.send_message(f"Failed to open status modal: {exc}", ephemeral=True)


class TaskEditSelect(discord.ui.Select):
    def __init__(self, tasks, board_id: str):
        self._board_id = board_id
        options = []
        for idx, task in enumerate(tasks[:25], start=1):
            options.append(
                discord.SelectOption(
                    label=f"{idx}. {task.title[:80]}",
                    description=f"{task.owner or 'Unassigned'} | {normalize_status(task.status)}",
                    value=str(idx),
                )
            )
        super().__init__(
            placeholder="Select a task to edit...",
            min_values=1,
            max_values=1,
            options=options,
            row=1,
        )

    async def callback(self, interaction: discord.Interaction):
        try:
            service, _cfg = get_board_service_and_config(self._board_id)
            selected_idx = int(self.values[0])
            tasks = await asyncio.to_thread(service.list_tasks)
            if selected_idx < 1 or selected_idx > len(tasks):
                await interaction.response.send_message("Selected task is out of range.", ephemeral=True)
                return
            chosen = tasks[selected_idx - 1]
            await interaction.response.send_modal(EditTaskModal(task_number=selected_idx, board_id=self._board_id, task=chosen))
        except Exception as exc:
            await interaction.response.send_message(f"Failed to open edit form: {exc}", ephemeral=True)


async def get_due_soon_channel():
    return await get_channel_by_id(DUE_SOON_CHANNEL_INT)


async def post_or_update_live_panel(board_config, tasks):
    global live_message_ids

    board_id = board_config.get("boardId") or board_config.get("id") or "default"
    board_name = board_config.get("todoBoardName") or board_config.get("boardName") or "TASK BOARD"
    live_channel_id = parse_channel_id(board_config.get("liveSyncChannelId"))
    if not live_channel_id:
        return

    channel, _ = await get_channel_by_id(live_channel_id)
    if channel is None:
        return

    content = build_dashboard_text(tasks, board_title=board_name)
    view = TaskControlView(tasks, str(board_id))

    message_id = live_message_ids.get(str(board_id))
    if message_id:
        try:
            msg = await channel.fetch_message(message_id)
            await msg.edit(content=content, embed=None, view=view)
            return
        except discord.NotFound:
            logger.warning(f"Panel message {message_id} not found, posting new one")
        except discord.Forbidden:
            logger.error(f"Permission denied editing panel message {message_id}")

    msg = await channel.send(content=content, view=view)
    live_message_ids[str(board_id)] = msg.id


async def live_sync_loop():
    global live_snapshots

    while True:
        sleep_seconds = SYNC_POLL_SECONDS
        try:
            configs = all_runtime_board_configs()
            if configs:
                sleep_seconds = min(int(cfg.get("syncPollSeconds", SYNC_POLL_SECONDS)) for cfg in configs)

            for cfg in configs:
                board_id = str(cfg.get("boardId") or cfg.get("id") or "default")
                service = create_service_for_config(cfg)
                tasks = await asyncio.to_thread(service.list_tasks)
                snapshot = "|".join(
                    f"{t.row}:{t.title}:{t.owner}:{normalize_status(t.status)}:{t.deadline}:{t.deliverable}:{t.notes}"
                    for t in tasks
                )
                if snapshot != live_snapshots.get(board_id):
                    await post_or_update_live_panel(cfg, tasks)
                    live_snapshots[board_id] = snapshot
        except asyncio.CancelledError:
            logger.info("Live sync loop cancelled")
            break
        except Exception as exc:
            logger.error(f"Live sync error: {exc}", exc_info=True)

        await asyncio.sleep(max(5, sleep_seconds))


async def send_due_soon_reminder(
    channel,
    tasks,
    force: bool = False,
    due_soon_days: int | None = None,
    interval_hours: int | None = None,
    board_name: str | None = None,
):
    days_window = due_soon_days if due_soon_days is not None else DUE_SOON_DAYS
    footer_hours = interval_hours if interval_hours is not None else DUE_SOON_INTERVAL_HOURS

    due_list = []
    now = datetime.now(UTC).date()
    for task in tasks:
        if normalize_status(task.status) == "Completed":
            continue
        due = parse_deadline(task.deadline)
        if due is None:
            continue
        days_left = (due - now).days
        if days_left <= days_window:
            due_list.append((task, days_left))
    if not due_list and not force:
        return False

    if not due_list and force:
        await channel.send(f"Everyone has no urgent or overdue tasks right now in task board {board_name}.")
        return True

    grouped = {}
    for task, days_left in due_list:
        owner_key = (task.owner or "").strip().lower() or "unassigned"
        grouped.setdefault(owner_key, []).append((task, days_left))

    ordered_owner_keys = []
    for name in OWNER_MENTION_ORDER:
        if name in grouped:
            ordered_owner_keys.append(name)
    for key in sorted(grouped.keys()):
        if key not in ordered_owner_keys:
            ordered_owner_keys.append(key)

    lines = []
    guild = getattr(channel, "guild", None)
    counter = 1
    for owner_key in ordered_owner_keys:
        owner_tasks = grouped.get(owner_key, [])
        if not owner_tasks:
            continue
        sample_owner = owner_tasks[0][0].owner
        mention = await resolve_owner_mention(sample_owner, guild)
        owner_text = mention if mention else sample_owner or "(no owner)"
        lines.append(f"{counter}. {owner_text}")
        for task, days_left in owner_tasks:
            if days_left < 0:
                urgency = f"OVERDUE by {abs(days_left)} day(s)"
            elif days_left == 0:
                urgency = "DUE TODAY"
            else:
                urgency = f"due in {days_left} day(s)"
            lines.append(
                f"   - {task.title} | {urgency} | due {format_deadline_for_ui(task.deadline)} | {normalize_status(task.status)}"
            )
        lines.append("")
        counter += 1

    embed = discord.Embed(
        title=f"Task Alert Board - {board_name}" if board_name else "Task Alert Board",
        description="\n".join(lines).strip(),
        color=discord.Color.red(),
        timestamp=datetime.now(UTC),
    )
    embed.set_footer(text=f"Includes overdue + due soon tasks. Auto reminder every {footer_hours} hour(s)")
    await channel.send(embed=embed)
    return True


async def due_soon_reminder_loop():
    global due_soon_last_sent

    while True:
        try:
            now = datetime.now(UTC)
            for cfg in all_runtime_board_configs():
                board_id = str(cfg.get("boardId") or cfg.get("id") or "default")
                interval_hours = int(cfg.get("dueSoonIntervalHours", DUE_SOON_INTERVAL_HOURS))
                interval_seconds = max(60, interval_hours * 3600)
                last_sent = due_soon_last_sent.get(board_id)
                if last_sent and (now - last_sent).total_seconds() < interval_seconds:
                    continue

                due_channel_id = parse_channel_id(cfg.get("dueSoonChannelId"))
                channel, _ = await get_channel_by_id(due_channel_id)
                if channel is None:
                    continue

                service = create_service_for_config(cfg)
                tasks = await asyncio.to_thread(service.list_tasks)
                sent = await send_due_soon_reminder(
                    channel,
                    tasks,
                    force=False,
                    due_soon_days=int(cfg.get("dueSoonDays", DUE_SOON_DAYS)),
                    interval_hours=interval_hours,
                    board_name=cfg.get("todoBoardName") or cfg.get("boardName") or "TASK BOARD",
                )
                if sent:
                    due_soon_last_sent[board_id] = now
        except asyncio.CancelledError:
            logger.info("Due-soon reminder loop cancelled")
            break
        except Exception as exc:
            logger.error(f"Due-soon reminder error: {exc}", exc_info=True)

        await asyncio.sleep(60)


@bot.event
async def on_ready():
    global slash_commands_synced
    global sync_task
    global due_soon_task

    logger.info(f"{bot.user.name} has connected to Discord!")
    logger.info("Google Sheets task control is ready.")

    if not slash_commands_synced:
        try:
            global_synced = await bot.tree.sync()
            logger.info(f"Globally synced {len(global_synced)} slash command(s).")

            guild_sync_count = 0
            for guild in bot.guilds:
                synced_for_guild = await bot.tree.sync(guild=guild)
                logger.info(f"Guild sync for {guild.name} ({guild.id}): {len(synced_for_guild)} command(s).")
                guild_sync_count += 1

            if guild_sync_count == 0:
                logger.info("No guilds found for per-guild slash sync.")

            slash_commands_synced = True
        except Exception as exc:
            logger.error(f"Slash command sync failed: {exc}", exc_info=True)

    for cfg in all_runtime_board_configs():
        try:
            service = create_service_for_config(cfg)
            tasks = await asyncio.to_thread(service.list_tasks)
            await post_or_update_live_panel(cfg, tasks)
        except Exception as exc:
            logger.error(f"Initial panel load failed for {cfg.get('boardId')}: {exc}", exc_info=True)

    if sync_task is None:
        sync_task = asyncio.create_task(live_sync_loop())
    if due_soon_task is None:
        due_soon_task = asyncio.create_task(due_soon_reminder_loop())


@bot.tree.command(name="commands", description="Show available slash and prefix commands")
async def commands_slash(interaction: discord.Interaction):
    lines = [
        "Slash commands:",
        "/commands - show this command list",
        "/panel - post interactive task panel",
        "/tasks - show current task board text",
        "/setstatus - set task status by task number",
        "/remindnow - send due-soon reminder now",
        "/testannounce - send test announcement to due-soon channel",
        "/boards - list configured boards",
        "/useboard - switch active board",
        "",
        "Prefix commands (still supported):",
        "!panel, !tasks, !create, !edit, !delete, !setstatus, !remindnow, !testannounce, !boards, !useboard",
    ]
    await interaction.response.send_message("\n".join(lines), ephemeral=True)


@bot.tree.command(name="boards", description="List boards from bot_boards.json")
async def boards_slash(interaction: discord.Interaction):
    boards, active_id = list_boards_from_file()
    if not boards:
        await interaction.response.send_message("No boards found in bot_boards.json.", ephemeral=True)
        return

    lines = []
    for board in boards:
        board_id = str(board.get("id", "")).strip()
        marker = "*" if board_id == active_id else " "
        name = board.get("name") or board.get("todoBoardName") or "(unnamed)"
        lines.append(f"{marker} {board_id} - {name}")
    await interaction.response.send_message("Boards:\n" + "\n".join(lines), ephemeral=True)


@bot.tree.command(name="useboard", description="Switch active board by id")
@discord.app_commands.describe(board_id="Board id from bot_boards.json")
async def useboard_slash(interaction: discord.Interaction, board_id: str):
    ok, err = switch_active_board(board_id.strip(), persist=True)
    if not ok:
        await interaction.response.send_message(f"Failed to switch board: {err}", ephemeral=True)
        return

    tasks = await asyncio.to_thread(todo_service.list_tasks)
    await post_or_update_live_panel(tasks)
    await interaction.response.send_message(
        f"Active board switched to {ACTIVE_BOARD_ID} ({TODO_BOARD_NAME}).",
        ephemeral=True,
    )


@bot.tree.command(name="panel", description="Post the interactive task panel")
async def panel_slash(interaction: discord.Interaction):
    try:
        tasks = await asyncio.to_thread(todo_service.list_tasks)
        await interaction.response.send_message(content=build_dashboard_text(tasks), view=TaskControlView(tasks, str(ACTIVE_BOARD_ID or "")))
    except Exception as exc:
        await interaction.response.send_message(f"Failed to build panel: {exc}", ephemeral=True)


@bot.tree.command(name="tasks", description="Show the current task board")
async def tasks_slash(interaction: discord.Interaction):
    try:
        tasks = await asyncio.to_thread(todo_service.list_tasks)
        await interaction.response.send_message(content=build_dashboard_text(tasks))
    except Exception as exc:
        await interaction.response.send_message(f"Failed to fetch tasks: {exc}", ephemeral=True)


@bot.tree.command(name="setstatus", description="Set a task status by task number")
@discord.app_commands.describe(task_number="Task number from the panel", status="New status")
@discord.app_commands.choices(
    status=[
        discord.app_commands.Choice(name="Not started", value="Not started"),
        discord.app_commands.Choice(name="In progress", value="In progress"),
        discord.app_commands.Choice(name="Blocked", value="Blocked"),
        discord.app_commands.Choice(name="Completed", value="Completed"),
    ]
)
async def setstatus_slash(
    interaction: discord.Interaction,
    task_number: int,
    status: discord.app_commands.Choice[str],
):
    try:
        await asyncio.to_thread(todo_service.set_task_status, task_number, status.value)
        tasks = await asyncio.to_thread(todo_service.list_tasks)
        await interaction.response.send_message(content=build_dashboard_text(tasks))
    except Exception as exc:
        await interaction.response.send_message(f"Failed to set status: {exc}", ephemeral=True)


@bot.tree.command(name="remindnow", description="Send due-soon reminder now")
async def remindnow_slash(interaction: discord.Interaction):
    try:
        await defer_interaction(interaction)
        sent = 0
        failed = []
        for cfg in all_runtime_board_configs():
            due_channel_id = parse_channel_id(cfg.get("dueSoonChannelId"))
            channel, reason = await get_channel_by_id(due_channel_id)
            if channel is None:
                failed.append(f"{cfg.get('boardId')}: {reason}")
                continue

            service = create_service_for_config(cfg)
            tasks = await asyncio.to_thread(service.list_tasks)
            await send_due_soon_reminder(
                channel,
                tasks,
                force=True,
                due_soon_days=int(cfg.get("dueSoonDays", DUE_SOON_DAYS)),
                interval_hours=int(cfg.get("dueSoonIntervalHours", DUE_SOON_INTERVAL_HOURS)),
                board_name=cfg.get("todoBoardName") or cfg.get("boardName") or "TASK BOARD",
            )
            sent += 1

        msg = f"Due-soon reminder sent for {sent} board(s)."
        if failed:
            msg += " Failed: " + " | ".join(failed)
        await interaction.followup.send(msg, ephemeral=True)
    except Exception as exc:
        await interaction.followup.send(f"Failed to send reminder: {exc}", ephemeral=True)


@bot.tree.command(name="testannounce", description="Send a test announcement to due-soon channel")
async def testannounce_slash(interaction: discord.Interaction):
    try:
        await defer_interaction(interaction)
        sent = 0
        failed = []
        for cfg in all_runtime_board_configs():
            due_channel_id = parse_channel_id(cfg.get("dueSoonChannelId"))
            channel, reason = await get_channel_by_id(due_channel_id)
            if channel is None:
                failed.append(f"{cfg.get('boardId')}: {reason}")
                continue

            service = create_service_for_config(cfg)
            tasks = await asyncio.to_thread(service.list_tasks)
            await send_due_soon_reminder(
                channel,
                tasks,
                force=True,
                due_soon_days=int(cfg.get("dueSoonDays", DUE_SOON_DAYS)),
                interval_hours=int(cfg.get("dueSoonIntervalHours", DUE_SOON_INTERVAL_HOURS)),
                board_name=cfg.get("todoBoardName") or cfg.get("boardName") or "TASK BOARD",
            )
            sent += 1

        msg = f"Test announcement sent for {sent} board(s)."
        if failed:
            msg += " Failed: " + " | ".join(failed)
        await interaction.followup.send(msg, ephemeral=True)
    except Exception as exc:
        await interaction.followup.send(f"Failed to send test announcement: {exc}", ephemeral=True)


@bot.command(name="panel")
async def panel_command(ctx):
    try:
        tasks = await asyncio.to_thread(todo_service.list_tasks)
        await ctx.send(content=build_dashboard_text(tasks), view=TaskControlView(tasks, str(ACTIVE_BOARD_ID or "")))
    except Exception as exc:
        await ctx.send(f"Failed to build panel: {exc}")


@bot.command(name="tasks")
async def tasks_command(ctx):
    try:
        tasks = await asyncio.to_thread(todo_service.list_tasks)
        await ctx.send(content=build_dashboard_text(tasks))
    except Exception as exc:
        await ctx.send(f"Failed to fetch tasks: {exc}")


@bot.command(name="create")
async def create_command(ctx, *, payload: str):
    try:
        parts = [p.strip() for p in payload.split("|")]
        while len(parts) < 6:
            parts.append("")
        title, owner, status, deadline, deliverable, notes = parts[:6]
        if not title:
            await ctx.send("Use: !create task | owner | status | deadline | deliverable | notes")
            return
        await asyncio.to_thread(todo_service.append_task, title, owner, status or "Not started", deadline, deliverable, notes)
        tasks = await asyncio.to_thread(todo_service.list_tasks)
        await ctx.send(content=build_dashboard_text(tasks))
    except Exception as exc:
        await ctx.send(f"Failed to create task: {exc}")


@bot.command(name="edit")
async def edit_command(ctx, task_number: int, field: str, *, value: str):
    try:
        field_key = field.strip().lower()
        allowed_fields = {"task": "title", "owner": "owner", "status": "status", "deadline": "deadline", "deliverable": "deliverable", "notes": "notes"}
        if field_key not in allowed_fields:
            await ctx.send("Field must be one of: task, owner, status, deadline, deliverable, notes")
            return

        kwargs = {allowed_fields[field_key]: value.strip()}
        await asyncio.to_thread(todo_service.update_task, task_number=task_number, **kwargs)
        tasks = await asyncio.to_thread(todo_service.list_tasks)
        await ctx.send(content=build_dashboard_text(tasks))
    except ValueError as exc:
        await ctx.send(str(exc))
    except Exception as exc:
        await ctx.send(f"Failed to edit task: {exc}")


@bot.command(name="delete")
async def delete_command(ctx, task_number: int):
    try:
        await asyncio.to_thread(todo_service.delete_task, task_number)
        tasks = await asyncio.to_thread(todo_service.list_tasks)
        await ctx.send(content=build_dashboard_text(tasks))
    except Exception as exc:
        await ctx.send(f"Failed to delete task: {exc}")


@bot.command(name="setstatus")
async def setstatus_command(ctx, task_number: int, *, status: str):
    try:
        normalized = parse_status_input(status)
        if normalized is None:
            await ctx.send("Status must be: Not started, In progress, Blocked, Completed")
            return
        await asyncio.to_thread(todo_service.set_task_status, task_number, normalized)
        tasks = await asyncio.to_thread(todo_service.list_tasks)
        await ctx.send(content=build_dashboard_text(tasks))
    except Exception as exc:
        await ctx.send(f"Failed to set status: {exc}")


@bot.command(name="remindnow")
async def remindnow_command(ctx):
    try:
        sent = 0
        failed = []
        for cfg in all_runtime_board_configs():
            due_channel_id = parse_channel_id(cfg.get("dueSoonChannelId"))
            channel, reason = await get_channel_by_id(due_channel_id)
            if channel is None:
                failed.append(f"{cfg.get('boardId')}: {reason}")
                continue

            service = create_service_for_config(cfg)
            tasks = await asyncio.to_thread(service.list_tasks)
            await send_due_soon_reminder(
                channel,
                tasks,
                force=True,
                due_soon_days=int(cfg.get("dueSoonDays", DUE_SOON_DAYS)),
                interval_hours=int(cfg.get("dueSoonIntervalHours", DUE_SOON_INTERVAL_HOURS)),
                board_name=cfg.get("todoBoardName") or cfg.get("boardName") or "TASK BOARD",
            )
            sent += 1

        msg = f"Due-soon reminder sent for {sent} board(s)."
        if failed:
            msg += " Failed: " + " | ".join(failed)
        await ctx.send(msg)
    except Exception as exc:
        await ctx.send(f"Failed to send reminder: {exc}")


@bot.command(name="testannounce")
async def testannounce_command(ctx):
    try:
        sent = 0
        failed = []
        for cfg in all_runtime_board_configs():
            due_channel_id = parse_channel_id(cfg.get("dueSoonChannelId"))
            channel, reason = await get_channel_by_id(due_channel_id)
            if channel is None:
                failed.append(f"{cfg.get('boardId')}: {reason}")
                continue

            service = create_service_for_config(cfg)
            tasks = await asyncio.to_thread(service.list_tasks)
            await send_due_soon_reminder(
                channel,
                tasks,
                force=True,
                due_soon_days=int(cfg.get("dueSoonDays", DUE_SOON_DAYS)),
                interval_hours=int(cfg.get("dueSoonIntervalHours", DUE_SOON_INTERVAL_HOURS)),
                board_name=cfg.get("todoBoardName") or cfg.get("boardName") or "TASK BOARD",
            )
            sent += 1

        msg = f"Test announcement sent for {sent} board(s)."
        if failed:
            msg += " Failed: " + " | ".join(failed)
        await ctx.send(msg)
    except Exception as exc:
        await ctx.send(f"Failed to send test announcement: {exc}")


@bot.command(name="boards")
async def boards_command(ctx):
    boards, active_id = list_boards_from_file()
    if not boards:
        await ctx.send("No boards found in bot_boards.json.")
        return

    lines = []
    for board in boards:
        board_id = str(board.get("id", "")).strip()
        marker = "*" if board_id == active_id else " "
        name = board.get("name") or board.get("todoBoardName") or "(unnamed)"
        lines.append(f"{marker} {board_id} - {name}")
    await ctx.send("Boards:\n" + "\n".join(lines))


@bot.command(name="useboard")
async def useboard_command(ctx, *, board_id: str):
    ok, err = switch_active_board(board_id.strip(), persist=True)
    if not ok:
        await ctx.send(f"Failed to switch board: {err}")
        return

    tasks = await asyncio.to_thread(todo_service.list_tasks)
    await post_or_update_live_panel(tasks)
    await ctx.send(f"Active board switched to {ACTIVE_BOARD_ID} ({TODO_BOARD_NAME}).")


@bot.event
async def on_close():
    """Gracefully cancel background tasks on bot close."""
    global sync_task, due_soon_task
    logger.info("Bot closing, cancelling background tasks...")
    if sync_task and not sync_task.done():
        sync_task.cancel()
    if due_soon_task and not due_soon_task.done():
        due_soon_task.cancel()


@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        return
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(
            "Missing argument. Commands: !panel, !tasks, !create, !edit, !delete, !setstatus, !remindnow, !testannounce, !boards, !useboard"
        )
        return
    await ctx.send(f"Command error: {error}")


def validate_env():
    missing = []
    if not DISCORD_TOKEN:
        missing.append("DISCORD_TOKEN")
    if not GOOGLE_SHEET_ID:
        missing.append("GOOGLE_SHEET_ID")
    if not os.path.exists(GOOGLE_SERVICE_ACCOUNT_FILE):
        missing.append("GOOGLE_SERVICE_ACCOUNT_FILE (file not found)")

    if missing:
        raise RuntimeError(f"Missing configuration: {', '.join(missing)}")


def main():
    global todo_service

    validate_env()
    ok, err = switch_active_board(str(ACTIVE_BOARD_ID or ""), persist=False)
    if not ok:
        raise RuntimeError(f"Failed to initialize active board: {err}")

    bot.run(DISCORD_TOKEN, log_handler=handler, log_level=logging.INFO)


if __name__ == "__main__":
    main()
