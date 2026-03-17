#!/usr/bin/env python3
"""
Trackie PB Fetcher — GUI
A modern desktop interface for scraping U SPORTS personal bests from Trackie.
"""

from __future__ import annotations

import csv
import os
import sys
import threading
import tkinter as tk
import webbrowser
from tkinter import filedialog, ttk
from typing import Optional

import customtkinter as ctk

from scrape_trackie_pbs import (
    CSV_FIELDNAMES,
    DEFAULT_UNIVERSITY_URL,
    run_scrape,
    write_csv,
)

# ---------------------------------------------------------------------------
# Theme palette — deep navy + warm orange accent
# ---------------------------------------------------------------------------
ACCENT = "#E85D2C"
ACCENT_HOVER = "#D14A1C"
ACCENT_LIGHT = "#FF7A47"
ACCENT_GLOW = "#FF9A6C"
BG_DARK = "#0D1117"
BG_SIDEBAR = "#161B22"
BG_CARD = "#1C2333"
BG_INPUT = "#0F3460"
BG_TABLE_ROW_ALT = "#141B2A"
BG_TABLE_ROW = "#1C2333"
BG_SELECTED = "#2A1810"
BORDER_SUBTLE = "#30363D"
TEXT_PRIMARY = "#E6EDF3"
TEXT_SECONDARY = "#8B949E"
TEXT_MUTED = "#484F58"
SUCCESS = "#3FB950"
ERROR = "#F85149"

FONT_FAMILY = "Segoe UI" if sys.platform == "win32" else "SF Pro Display"
MONO_FAMILY = "SF Mono" if sys.platform == "darwin" else "Consolas"

TABLE_COLUMNS = [
    ("athlete_name", "Athlete", 170),
    ("sex", "Sex", 45),
    ("event", "Event", 140),
    ("pb_raw", "Performance", 105),
    ("pb_unit", "Unit", 45),
    ("better_is", "Direction", 68),
    ("pb_date", "Date", 95),
    ("pb_season", "Season", 75),
    ("pb_meet", "Meet", 240),
]


class App(ctk.CTk):
    def __init__(self) -> None:
        super().__init__()

        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("dark-blue")

        self.title("Trackie PB Fetcher")
        self.geometry("1340x820")
        self.minsize(1000, 660)

        self._data: list[dict] = []
        self._filtered: list[dict] = []
        self._sort_col: Optional[str] = None
        self._sort_asc: bool = True
        self._scrape_thread: Optional[threading.Thread] = None
        self._cancel_flag = threading.Event()

        self._build_ui()
        self._bind_shortcuts()

    # ------------------------------------------------------------------
    # Keyboard shortcuts
    # ------------------------------------------------------------------
    def _bind_shortcuts(self) -> None:
        mod = "Command" if sys.platform == "darwin" else "Control"
        self.bind(f"<{mod}-e>", lambda _: self._on_export())
        self.bind(f"<{mod}-r>", lambda _: self._on_scrape())
        self.bind(f"<{mod}-l>", lambda _: self._on_load_csv())

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------
    def _build_ui(self) -> None:
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)
        self.configure(fg_color=BG_DARK)

        self._build_sidebar()
        self._build_main()

    # ── Sidebar ──────────────────────────────────────────────────────
    def _build_sidebar(self) -> None:
        sidebar = ctk.CTkFrame(self, width=300, corner_radius=0, fg_color=BG_SIDEBAR,
                               border_width=0)
        sidebar.grid(row=0, column=0, sticky="nsew")
        sidebar.grid_propagate(False)
        sidebar.grid_columnconfigure(0, weight=1)

        # Brand
        brand = ctk.CTkFrame(sidebar, fg_color="transparent")
        brand.grid(row=0, column=0, padx=24, pady=(28, 0), sticky="ew")

        ctk.CTkLabel(
            brand, text="\u26A1  TRACKIE",
            font=ctk.CTkFont(family=FONT_FAMILY, size=22, weight="bold"),
            text_color=ACCENT,
        ).pack(anchor="w")
        ctk.CTkLabel(
            brand, text="Personal Best Fetcher",
            font=ctk.CTkFont(family=FONT_FAMILY, size=13),
            text_color=TEXT_MUTED,
        ).pack(anchor="w", pady=(2, 0))

        # Thin separator
        ctk.CTkFrame(sidebar, height=1, fg_color=BORDER_SUBTLE).grid(
            row=1, column=0, padx=24, pady=16, sticky="ew")

        # Scrollable settings area
        settings = ctk.CTkFrame(sidebar, fg_color="transparent")
        settings.grid(row=2, column=0, padx=24, sticky="nsew")
        settings.grid_columnconfigure(0, weight=1)

        r = 0

        # Section label
        ctk.CTkLabel(settings, text="CONFIGURATION",
                     font=ctk.CTkFont(family=FONT_FAMILY, size=10, weight="bold"),
                     text_color=TEXT_MUTED).grid(row=r, column=0, sticky="w", pady=(0, 10))
        r += 1

        # University URL
        r = self._add_field_label(settings, "University URL", r)
        self._url_var = ctk.StringVar(value=DEFAULT_UNIVERSITY_URL)
        url_entry = ctk.CTkEntry(
            settings, textvariable=self._url_var, height=34, corner_radius=8,
            fg_color=BG_CARD, border_color=BORDER_SUBTLE, border_width=1,
            text_color=TEXT_PRIMARY,
            placeholder_text="https://www.trackie.com/usports/...",
            placeholder_text_color=TEXT_MUTED,
            font=ctk.CTkFont(family=FONT_FAMILY, size=12),
        )
        url_entry.grid(row=r, column=0, sticky="ew", pady=(0, 12)); r += 1

        # Seasons back — slider with value badge
        r = self._add_field_label(settings, "Seasons Back", r)
        sf = ctk.CTkFrame(settings, fg_color="transparent")
        sf.grid(row=r, column=0, sticky="ew", pady=(0, 12)); r += 1
        sf.grid_columnconfigure(0, weight=1)

        self._years_var = ctk.IntVar(value=5)
        self._years_badge = ctk.CTkLabel(
            sf, text="5", width=32, height=24, corner_radius=6,
            fg_color=ACCENT, text_color="#FFFFFF",
            font=ctk.CTkFont(family=MONO_FAMILY, size=12, weight="bold"),
        )
        self._years_badge.grid(row=0, column=1, padx=(10, 0))
        ctk.CTkSlider(
            sf, from_=1, to=10, number_of_steps=9, variable=self._years_var,
            command=self._on_years_changed,
            button_color=ACCENT, button_hover_color=ACCENT_HOVER,
            progress_color=ACCENT, fg_color=BG_CARD, height=14,
        ).grid(row=0, column=0, sticky="ew")

        # Workers
        r = self._add_field_label(settings, "Workers", r)
        wf = ctk.CTkFrame(settings, fg_color="transparent")
        wf.grid(row=r, column=0, sticky="ew", pady=(0, 12)); r += 1
        wf.grid_columnconfigure(0, weight=1)

        self._workers_var = ctk.IntVar(value=6)
        self._workers_badge = ctk.CTkLabel(
            wf, text="6", width=32, height=24, corner_radius=6,
            fg_color=ACCENT, text_color="#FFFFFF",
            font=ctk.CTkFont(family=MONO_FAMILY, size=12, weight="bold"),
        )
        self._workers_badge.grid(row=0, column=1, padx=(10, 0))
        ctk.CTkSlider(
            wf, from_=1, to=12, number_of_steps=11, variable=self._workers_var,
            command=self._on_workers_changed,
            button_color=ACCENT, button_hover_color=ACCENT_HOVER,
            progress_color=ACCENT, fg_color=BG_CARD, height=14,
        ).grid(row=0, column=0, sticky="ew")

        # Toggle
        self._past_var = ctk.BooleanVar(value=True)
        ctk.CTkSwitch(
            settings, text="Include past athletes", variable=self._past_var,
            font=ctk.CTkFont(family=FONT_FAMILY, size=12),
            text_color=TEXT_SECONDARY, button_color=ACCENT,
            button_hover_color=ACCENT_HOVER, progress_color=ACCENT,
            fg_color=BG_CARD, height=24,
        ).grid(row=r, column=0, sticky="w", pady=(4, 0)); r += 1

        # Separator
        ctk.CTkFrame(sidebar, height=1, fg_color=BORDER_SUBTLE).grid(
            row=3, column=0, padx=24, pady=16, sticky="ew")

        # Action buttons
        actions = ctk.CTkFrame(sidebar, fg_color="transparent")
        actions.grid(row=4, column=0, padx=24, sticky="ew")
        actions.grid_columnconfigure(0, weight=1)

        self._run_btn = ctk.CTkButton(
            actions, text="\u25B6  Start Scraping",
            font=ctk.CTkFont(family=FONT_FAMILY, size=13, weight="bold"),
            height=42, corner_radius=10, fg_color=ACCENT,
            hover_color=ACCENT_HOVER, command=self._on_scrape,
        )
        self._run_btn.grid(row=0, column=0, sticky="ew", pady=(0, 8))

        self._cancel_btn = ctk.CTkButton(
            actions, text="Cancel",
            font=ctk.CTkFont(family=FONT_FAMILY, size=12),
            height=34, corner_radius=8, fg_color=BG_CARD,
            border_color=BORDER_SUBTLE, border_width=1,
            hover_color="#2D1B1B", text_color=TEXT_SECONDARY,
            command=self._on_cancel, state="disabled",
        )
        self._cancel_btn.grid(row=1, column=0, sticky="ew", pady=(0, 8))

        btn_row = ctk.CTkFrame(actions, fg_color="transparent")
        btn_row.grid(row=2, column=0, sticky="ew")
        btn_row.grid_columnconfigure((0, 1), weight=1)

        self._export_btn = ctk.CTkButton(
            btn_row, text="Export CSV",
            font=ctk.CTkFont(family=FONT_FAMILY, size=12),
            height=34, corner_radius=8, fg_color=BG_CARD,
            border_color=BORDER_SUBTLE, border_width=1,
            hover_color="#1E3A5F", text_color=TEXT_SECONDARY,
            command=self._on_export, state="disabled",
        )
        self._export_btn.grid(row=0, column=0, sticky="ew", padx=(0, 4))

        self._load_btn = ctk.CTkButton(
            btn_row, text="Load CSV",
            font=ctk.CTkFont(family=FONT_FAMILY, size=12),
            height=34, corner_radius=8, fg_color=BG_CARD,
            border_color=BORDER_SUBTLE, border_width=1,
            hover_color="#1E3A5F", text_color=TEXT_SECONDARY,
            command=self._on_load_csv,
        )
        self._load_btn.grid(row=0, column=1, sticky="ew", padx=(4, 0))

        # Progress area
        prog = ctk.CTkFrame(sidebar, fg_color="transparent")
        prog.grid(row=5, column=0, padx=24, pady=(16, 0), sticky="ew")
        prog.grid_columnconfigure(0, weight=1)

        self._progress_bar = ctk.CTkProgressBar(
            prog, height=4, corner_radius=2,
            fg_color=BG_CARD, progress_color=ACCENT,
        )
        self._progress_bar.grid(row=0, column=0, sticky="ew")
        self._progress_bar.set(0)

        self._progress_label = ctk.CTkLabel(
            prog, text="Ready",
            font=ctk.CTkFont(family=FONT_FAMILY, size=11),
            text_color=TEXT_MUTED, anchor="w",
        )
        self._progress_label.grid(row=1, column=0, sticky="w", pady=(4, 0))

        # Push stats to bottom
        sidebar.grid_rowconfigure(6, weight=1)

        # Stats card
        stats_card = ctk.CTkFrame(sidebar, fg_color=BG_CARD, corner_radius=10,
                                  border_width=1, border_color=BORDER_SUBTLE)
        stats_card.grid(row=7, column=0, padx=24, pady=(0, 24), sticky="sew")
        stats_card.grid_columnconfigure((0, 1, 2), weight=1)

        self._stat_athletes = self._make_stat(stats_card, "Athletes", "—", 0)
        self._stat_events = self._make_stat(stats_card, "Events", "—", 1)
        self._stat_records = self._make_stat(stats_card, "Records", "—", 2)

    def _add_field_label(self, parent: ctk.CTkFrame, text: str, row: int) -> int:
        ctk.CTkLabel(
            parent, text=text,
            font=ctk.CTkFont(family=FONT_FAMILY, size=12),
            text_color=TEXT_SECONDARY,
        ).grid(row=row, column=0, sticky="w", pady=(0, 4))
        return row + 1

    def _make_stat(self, parent: ctk.CTkFrame, label: str, value: str, col: int) -> ctk.CTkLabel:
        cell = ctk.CTkFrame(parent, fg_color="transparent")
        cell.grid(row=0, column=col, padx=8, pady=14)
        val_lbl = ctk.CTkLabel(
            cell, text=value,
            font=ctk.CTkFont(family=MONO_FAMILY, size=22, weight="bold"),
            text_color=ACCENT_GLOW,
        )
        val_lbl.pack()
        ctk.CTkLabel(
            cell, text=label.upper(),
            font=ctk.CTkFont(family=FONT_FAMILY, size=9, weight="bold"),
            text_color=TEXT_MUTED,
        ).pack(pady=(2, 0))
        return val_lbl

    # ── Main content area ────────────────────────────────────────────
    def _build_main(self) -> None:
        main = ctk.CTkFrame(self, corner_radius=0, fg_color=BG_DARK)
        main.grid(row=0, column=1, sticky="nsew")
        main.grid_columnconfigure(0, weight=1)
        main.grid_rowconfigure(1, weight=1)

        # ─ Toolbar ─
        toolbar = ctk.CTkFrame(main, fg_color="transparent")
        toolbar.grid(row=0, column=0, padx=24, pady=(18, 10), sticky="ew")
        toolbar.grid_columnconfigure(1, weight=1)

        # Title + count badge
        title_frame = ctk.CTkFrame(toolbar, fg_color="transparent")
        title_frame.grid(row=0, column=0, sticky="w")

        ctk.CTkLabel(
            title_frame, text="Results",
            font=ctk.CTkFont(family=FONT_FAMILY, size=20, weight="bold"),
            text_color=TEXT_PRIMARY,
        ).pack(side="left")

        self._count_badge = ctk.CTkLabel(
            title_frame, text="0", width=40, height=22, corner_radius=11,
            fg_color=BG_CARD, text_color=TEXT_MUTED,
            font=ctk.CTkFont(family=MONO_FAMILY, size=11, weight="bold"),
        )
        self._count_badge.pack(side="left", padx=(10, 0))

        # Search + filters
        controls = ctk.CTkFrame(toolbar, fg_color="transparent")
        controls.grid(row=0, column=1, sticky="e")

        self._search_var = ctk.StringVar()
        self._search_var.trace_add("write", lambda *_: self._apply_filter())
        ctk.CTkEntry(
            controls, textvariable=self._search_var, width=240, height=34,
            corner_radius=17, fg_color=BG_CARD, border_color=BORDER_SUBTLE,
            border_width=1, text_color=TEXT_PRIMARY,
            placeholder_text="\U0001F50D  Search athletes, events...",
            placeholder_text_color=TEXT_MUTED,
            font=ctk.CTkFont(family=FONT_FAMILY, size=12),
        ).pack(side="left", padx=(0, 8))

        self._sex_filter = ctk.CTkSegmentedButton(
            controls, values=["All", "M", "F"], width=140, height=32,
            font=ctk.CTkFont(family=FONT_FAMILY, size=12),
            selected_color=ACCENT, selected_hover_color=ACCENT_HOVER,
            unselected_color=BG_CARD, unselected_hover_color="#1E3040",
            text_color=TEXT_PRIMARY, text_color_disabled=TEXT_MUTED,
            fg_color=BG_CARD, corner_radius=8,
            command=self._on_sex_filter,
        )
        self._sex_filter.set("All")
        self._sex_filter.pack(side="left", padx=(0, 8))

        self._event_filter = ctk.CTkComboBox(
            controls, values=["All Events"], width=180, height=34,
            corner_radius=8, fg_color=BG_CARD, border_color=BORDER_SUBTLE,
            border_width=1, button_color=BG_INPUT, button_hover_color=ACCENT,
            dropdown_fg_color=BG_CARD, dropdown_hover_color=BG_INPUT,
            text_color=TEXT_PRIMARY, dropdown_text_color=TEXT_PRIMARY,
            font=ctk.CTkFont(family=FONT_FAMILY, size=12),
            command=lambda _: self._apply_filter(), state="readonly",
        )
        self._event_filter.set("All Events")
        self._event_filter.pack(side="left")

        # ─ Table ─
        table_container = ctk.CTkFrame(main, fg_color=BG_CARD, corner_radius=12,
                                       border_width=1, border_color=BORDER_SUBTLE)
        table_container.grid(row=1, column=0, padx=24, pady=(0, 12), sticky="nsew")
        table_container.grid_columnconfigure(0, weight=1)
        table_container.grid_rowconfigure(0, weight=1)

        self._setup_table_style()

        col_ids = [c[0] for c in TABLE_COLUMNS]
        self._tree = ttk.Treeview(
            table_container, columns=col_ids, show="headings",
            style="Custom.Treeview", selectmode="browse",
        )
        self._tree.tag_configure("oddrow", background=BG_TABLE_ROW_ALT)
        self._tree.tag_configure("evenrow", background=BG_TABLE_ROW)

        for col_id, col_label, col_width in TABLE_COLUMNS:
            anchor = "w" if col_id in ("athlete_name", "event", "pb_meet") else "center"
            self._tree.heading(col_id, text=col_label,
                               command=lambda c=col_id: self._on_sort(c))
            self._tree.column(col_id, width=col_width, minwidth=30, anchor=anchor)

        scrollbar = ctk.CTkScrollbar(
            table_container, command=self._tree.yview,
            button_color=BG_INPUT, button_hover_color=ACCENT,
        )
        self._tree.configure(yscrollcommand=scrollbar.set)
        self._tree.grid(row=0, column=0, sticky="nsew", padx=(1, 0), pady=1)
        scrollbar.grid(row=0, column=1, sticky="ns", padx=(0, 1), pady=1)

        self._tree.bind("<Double-1>", self._on_row_double_click)

        # ─ Status bar ─
        status = ctk.CTkFrame(main, fg_color=BG_SIDEBAR, height=30, corner_radius=0)
        status.grid(row=2, column=0, sticky="sew")
        status.grid_columnconfigure(0, weight=1)

        self._status_label = ctk.CTkLabel(
            status, text="Configure settings and press Start Scraping  \u2014  Double-click a row to open athlete page",
            font=ctk.CTkFont(family=FONT_FAMILY, size=11),
            text_color=TEXT_MUTED, anchor="w",
        )
        self._status_label.grid(row=0, column=0, padx=16, pady=5, sticky="w")

        shortcut_text = "\u2318R Scrape  \u2318E Export  \u2318L Load" if sys.platform == "darwin" else "Ctrl+R Scrape  Ctrl+E Export  Ctrl+L Load"
        ctk.CTkLabel(
            status, text=shortcut_text,
            font=ctk.CTkFont(family=MONO_FAMILY, size=10),
            text_color=TEXT_MUTED, anchor="e",
        ).grid(row=0, column=1, padx=16, pady=5, sticky="e")

    def _setup_table_style(self) -> None:
        style = ttk.Style()
        style.theme_use("clam")

        style.configure(
            "Custom.Treeview",
            background=BG_TABLE_ROW,
            foreground=TEXT_PRIMARY,
            fieldbackground=BG_TABLE_ROW,
            borderwidth=0,
            font=(FONT_FAMILY, 12),
            rowheight=34,
        )
        style.configure(
            "Custom.Treeview.Heading",
            background="#111927",
            foreground=TEXT_SECONDARY,
            borderwidth=0,
            font=(FONT_FAMILY, 11, "bold"),
            relief="flat",
            padding=(10, 8),
        )
        style.map(
            "Custom.Treeview",
            background=[("selected", BG_SELECTED)],
            foreground=[("selected", ACCENT_LIGHT)],
        )
        style.map(
            "Custom.Treeview.Heading",
            background=[("active", "#192236")],
        )
        style.layout("Custom.Treeview",
                      [("Custom.Treeview.treearea", {"sticky": "nswe"})])

    # ------------------------------------------------------------------
    # Callbacks
    # ------------------------------------------------------------------
    def _on_years_changed(self, val: float) -> None:
        self._years_badge.configure(text=str(int(val)))

    def _on_workers_changed(self, val: float) -> None:
        self._workers_badge.configure(text=str(int(val)))

    def _on_sex_filter(self, _: str) -> None:
        self._apply_filter()

    def _on_scrape(self) -> None:
        if self._scrape_thread and self._scrape_thread.is_alive():
            return
        self._cancel_flag.clear()
        self._run_btn.configure(state="disabled", text="\u23F3  Scraping...")
        self._cancel_btn.configure(state="normal")
        self._export_btn.configure(state="disabled")
        self._progress_bar.set(0)
        self._set_status("Initializing scraper...")

        self._scrape_thread = threading.Thread(target=self._scrape_worker, daemon=True)
        self._scrape_thread.start()

    def _on_cancel(self) -> None:
        self._cancel_flag.set()
        self._cancel_btn.configure(state="disabled")
        self._set_status("Cancelling...")

    def _on_export(self) -> None:
        if not self._data:
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")],
            initialfile="trackie_pbs.csv",
        )
        if not path:
            return
        try:
            data = self._filtered if self._filtered else self._data
            write_csv(data, path)
            n = len(data)
            self._export_btn.configure(text="\u2713 Exported!", fg_color=SUCCESS, text_color="#FFFFFF")
            self._set_status(f"Exported {n} rows to {os.path.basename(path)}")
            self.after(2000, lambda: self._export_btn.configure(
                text="Export CSV", fg_color=BG_CARD, text_color=TEXT_SECONDARY))
        except Exception as e:
            self._set_status(f"Export failed: {e}")

    def _on_load_csv(self) -> None:
        path = filedialog.askopenfilename(
            filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")],
        )
        if not path:
            return
        try:
            with open(path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                self._data = list(reader)
            self._update_event_filter()
            self._apply_filter()
            self._update_stats()
            self._export_btn.configure(state="normal")
            self._set_status(f"Loaded {len(self._data)} records from {os.path.basename(path)}")
        except Exception as e:
            self._set_status(f"Failed to load CSV: {e}")

    def _on_sort(self, col: str) -> None:
        if self._sort_col == col:
            self._sort_asc = not self._sort_asc
        else:
            self._sort_col = col
            self._sort_asc = True

        for cid, clabel, _ in TABLE_COLUMNS:
            arrow = ""
            if cid == col:
                arrow = "  \u25B4" if self._sort_asc else "  \u25BE"
            self._tree.heading(cid, text=clabel + arrow)

        self._refresh_table()

    def _on_row_double_click(self, event: tk.Event) -> None:
        item = self._tree.identify_row(event.y)
        if not item:
            return
        idx = self._tree.index(item)
        if 0 <= idx < len(self._filtered):
            url = self._filtered[idx].get("athlete_url", "")
            if url:
                webbrowser.open(url)

    # ------------------------------------------------------------------
    # Background scrape
    # ------------------------------------------------------------------
    def _scrape_worker(self) -> None:
        def on_progress(done: int, total: int) -> None:
            if self._cancel_flag.is_set():
                raise KeyboardInterrupt("Cancelled by user")
            frac = done / total if total else 0
            self.after(0, self._progress_bar.set, frac)
            pct = int(frac * 100)
            self.after(0, self._progress_label.configure,
                       {"text": f"Processing athletes  {done}/{total}  ({pct}%)"})

        def on_status(msg: str) -> None:
            self.after(0, self._set_status, msg)

        try:
            rows = run_scrape(
                university_url=self._url_var.get().strip(),
                years_back=self._years_var.get(),
                include_past_athletes=self._past_var.get(),
                delay_seconds=0.6,
                max_workers=self._workers_var.get(),
                on_progress=on_progress,
                on_status=on_status,
            )
            self.after(0, self._on_scrape_done, rows)
        except KeyboardInterrupt:
            self.after(0, self._set_status, "Scraping cancelled.")
            self.after(0, self._scrape_cleanup)
        except Exception as e:
            self.after(0, self._set_status, f"Error: {e}")
            self.after(0, self._scrape_cleanup)

    def _on_scrape_done(self, rows: list[dict]) -> None:
        self._data = rows
        self._update_event_filter()
        self._apply_filter()
        self._update_stats()
        self._progress_bar.set(1.0)
        self._set_status(f"Loaded {len(rows)} personal best records.")
        self._scrape_cleanup()
        self._export_btn.configure(state="normal")

    def _scrape_cleanup(self) -> None:
        self._run_btn.configure(state="normal", text="\u25B6  Start Scraping")
        self._cancel_btn.configure(state="disabled")

    # ------------------------------------------------------------------
    # Filter / sort / display
    # ------------------------------------------------------------------
    def _apply_filter(self) -> None:
        query = self._search_var.get().strip().lower()
        sex = self._sex_filter.get()
        event = self._event_filter.get()

        self._filtered = []
        for row in self._data:
            if sex != "All" and row.get("sex", "") != sex:
                continue
            if event != "All Events" and row.get("event", "") != event:
                continue
            if query:
                searchable = " ".join(
                    str(row.get(k, ""))
                    for k in ("athlete_name", "event", "pb_raw", "pb_meet", "pb_season")
                ).lower()
                if query not in searchable:
                    continue
            self._filtered.append(row)

        self._refresh_table()
        self._update_stats()

    def _refresh_table(self) -> None:
        self._tree.delete(*self._tree.get_children())

        rows = list(self._filtered)
        if self._sort_col:
            def sort_key(r: dict):
                v = r.get(self._sort_col, "")
                if self._sort_col in ("pb_value", "pb_raw"):
                    try:
                        return float(str(v).rstrip("m").rstrip("*").strip())
                    except (ValueError, TypeError):
                        return 0.0
                return str(v).lower()
            rows.sort(key=sort_key, reverse=not self._sort_asc)

        for i, row in enumerate(rows):
            values = tuple(row.get(c[0], "") for c in TABLE_COLUMNS)
            tag = "oddrow" if i % 2 else "evenrow"
            self._tree.insert("", "end", values=values, tags=(tag,))

        count = len(rows)
        self._count_badge.configure(text=str(count))
        if count == len(self._data):
            self._status_label.configure(text=f"Showing all {count} records")
        else:
            self._status_label.configure(
                text=f"Showing {count} of {len(self._data)} records (filtered)")

    def _update_event_filter(self) -> None:
        events = sorted({r.get("event", "") for r in self._data if r.get("event")})
        self._event_filter.configure(values=["All Events"] + events)
        self._event_filter.set("All Events")

    def _update_stats(self) -> None:
        data = self._filtered if self._filtered else self._data
        athletes = len({r.get("athlete_name") for r in data if r.get("athlete_name")})
        events = len({r.get("event") for r in data if r.get("event")})
        records = len(data)

        self._stat_athletes.configure(text=str(athletes) if athletes else "\u2014")
        self._stat_events.configure(text=str(events) if events else "\u2014")
        self._stat_records.configure(text=str(records) if records else "\u2014")

    def _set_status(self, msg: str) -> None:
        self._progress_label.configure(text=msg)


def main() -> None:
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()
