import io
import json
import os
import queue
import sys
import threading
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

if getattr(sys, "frozen", False):
    bundled_base = Path(getattr(sys, "_MEIPASS", ""))
    if bundled_base and (bundled_base / "tkinter").exists():
        sys.path.insert(0, str(bundled_base))

import tkinter as tk
from tkinter import filedialog, messagebox, ttk


@dataclass(frozen=True)
class GuiConfig:
    app_name: str
    icon_filename: str
    settings_filename: str
    log_filename: str
    splash_version_text: str
    splash_done_token: str


THEMES = {
    "light": {
        "app_bg": "#f3f4f6",
        "header_bg": "#f8fafc",
        "card_bg": "#e9eef5",
        "panel_bg": "#ffffff",
        "text": "#111827",
        "subtext": "#6b7280",
        "accent": "#2563eb",
        "accent_soft": "#dbeafe",
        "muted_btn": "#e5edf8",
        "log_bg": "#0b1220",
        "log_fg": "#dbeafe",
        "nav_bg": "#f8fafc",
        "border": "#d7deea",
        "danger": "#d97706",
    },
    "dark": {
        "app_bg": "#0f172a",
        "header_bg": "#111827",
        "card_bg": "#172133",
        "panel_bg": "#0b1220",
        "text": "#f3f4f6",
        "subtext": "#94a3b8",
        "accent": "#60a5fa",
        "accent_soft": "#1d4ed8",
        "muted_btn": "#243247",
        "log_bg": "#020617",
        "log_fg": "#cbd5e1",
        "nav_bg": "#111827",
        "border": "#334155",
        "danger": "#f59e0b",
    },
}


def get_app_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def show_splash(config: GuiConfig) -> None:
    try:
        splash = tk.Tk()
        splash.title("Loading...")
        w, h = 400, 200
        sw = splash.winfo_screenwidth()
        sh = splash.winfo_screenheight()
        splash.geometry(f"{w}x{h}+{(sw-w)//2}+{(sh-h)//2}")
        splash.overrideredirect(True)
        splash.configure(bg="#111827")
        tk.Label(splash, text="TOVIS BU DATA 정리 도구", font=("Malgun Gothic", 16, "bold"), fg="white", bg="#111827").pack(pady=(40, 10))
        tk.Label(splash, text=config.splash_version_text, font=("Malgun Gothic", 10), fg="#94A3B8", bg="#111827").pack()
        pb = ttk.Progressbar(splash, mode="indeterminate", length=300)
        pb.pack(pady=20)
        pb.start(15)

        def check() -> None:
            if os.path.exists(config.splash_done_token):
                try:
                    os.remove(config.splash_done_token)
                except OSError:
                    pass
                splash.destroy()
            else:
                splash.after(100, check)

        splash.after(100, check)
        splash.mainloop()
    except Exception:
        return


class QueueWriter(io.TextIOBase):
    def __init__(self, log_queue: queue.Queue, log_path: Path):
        self.log_queue = log_queue
        self.log_path = log_path
        self._buffer = ""

    def _write_line(self, text: str) -> None:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{timestamp}] {text}\n"
        self.log_queue.put(line)
        with self.log_path.open("a", encoding="utf-8") as f:
            f.write(line)

    def write(self, s: str) -> int:
        if not s:
            return 0
        self._buffer += s
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            if line.strip():
                self._write_line(line)
        return len(s)

    def flush(self) -> None:
        if self._buffer.strip():
            self._write_line(self._buffer.rstrip())
        self._buffer = ""


class BUOrganizeApp:
    def __init__(self, root: tk.Tk, config: GuiConfig, run_pipeline_func, pipeline_cancelled_exc):
        self.root = root
        self.config = config
        self.run_pipeline_func = run_pipeline_func
        self.pipeline_cancelled_exc = pipeline_cancelled_exc
        self.icon_path = Path(__file__).with_name(config.icon_filename)
        self.settings_path = Path(__file__).with_name(config.settings_filename)
        self.log_path = get_app_base_dir() / config.log_filename

        self.root.title(config.app_name)
        self.root.geometry("1120x860")
        self.root.minsize(980, 760)
        self._apply_icon()

        self.log_queue: queue.Queue[str] = queue.Queue()
        self.worker: threading.Thread | None = None
        self.cancel_requested = threading.Event()
        self.latest_result: dict | None = None

        self.settings = self._load_settings()
        self.theme_name_var = tk.StringVar(value=self.settings.get("theme", "light"))
        self.auto_open_var = tk.StringVar(value=self.settings.get("auto_open", "none"))
        self.remember_paths_var = tk.BooleanVar(value=bool(self.settings.get("remember_paths", True)))
        self.default_threshold_var = tk.StringVar(value=str(self.settings.get("default_threshold", 12)))
        self.default_padding_var = tk.StringVar(value=str(self.settings.get("default_padding", 8)))

        self.image_root_var = tk.StringVar(value=self.settings.get("image_root", ""))
        self.data_root_var = tk.StringVar(value=self.settings.get("data_root", ""))
        self.threshold_var = tk.StringVar(value=str(self.settings.get("default_threshold", 12)))
        self.padding_var = tk.StringVar(value=str(self.settings.get("default_padding", 8)))
        self.status_var = tk.StringVar(value="대기 중")
        self.result_var = tk.StringVar(value="결과 파일 경로가 여기에 표시됩니다.")
        self.log_path_var = tk.StringVar(value=str(self.log_path))
        self.session_note_var = tk.StringVar(value="실행 대기")

        self.nav_buttons: dict[str, ttk.Button] = {}
        self.view_frames: dict[str, ttk.Frame] = {}
        self.scroll_canvases: dict[str, tk.Canvas] = {}
        self.current_view = "main"
        self.status_badge = None
        self.log_text = None

        self._build_ui()
        self._apply_theme()
        self.root.bind_all("<MouseWheel>", self._on_mousewheel)
        self._poll_log_queue()

        try:
            Path(self.config.splash_done_token).touch()
        except OSError:
            pass

    def _load_settings(self) -> dict:
        settings = {
            "theme": "light",
            "auto_open": "none",
            "remember_paths": True,
            "default_threshold": 12,
            "default_padding": 8,
            "image_root": "",
            "data_root": "",
        }
        if self.settings_path.exists():
            try:
                settings.update(json.loads(self.settings_path.read_text(encoding="utf-8")))
            except (OSError, json.JSONDecodeError):
                pass
        return settings

    def _save_settings(self) -> None:
        self.settings.update({
            "theme": self.theme_name_var.get(),
            "auto_open": self.auto_open_var.get(),
            "remember_paths": self.remember_paths_var.get(),
            "default_threshold": int(self.default_threshold_var.get() or 12),
            "default_padding": int(self.default_padding_var.get() or 8),
        })
        if self.remember_paths_var.get():
            self.settings["image_root"] = self.image_root_var.get().strip()
            self.settings["data_root"] = self.data_root_var.get().strip()
        else:
            self.settings["image_root"] = ""
            self.settings["data_root"] = ""
        try:
            self.settings_path.write_text(json.dumps(self.settings, ensure_ascii=False, indent=2), encoding="utf-8")
        except OSError:
            pass

    def _apply_icon(self) -> None:
        if self.icon_path.exists():
            try:
                self.root.iconbitmap(str(self.icon_path))
            except tk.TclError:
                pass

    def _build_ui(self) -> None:
        self.style = ttk.Style()
        self.style.theme_use("clam")
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(1, weight=1)

        self.header = ttk.Frame(self.root, padding=(18, 12))
        self.header.grid(row=0, column=0, sticky="ew")
        self.header.columnconfigure(1, weight=1)
        self.header_icon = tk.Label(self.header, text="▣", font=("Malgun Gothic", 16, "bold"))
        self.header_icon.grid(row=0, column=0, sticky="w")
        self.header_title = ttk.Label(self.header, text=self.config.app_name, font=("Malgun Gothic", 20, "bold"))
        self.header_title.grid(row=0, column=1, sticky="w", padx=(10, 0))
        self.header_status = ttk.Label(self.header, textvariable=self.status_var, font=("Malgun Gothic", 11, "bold"))
        self.header_status.grid(row=0, column=2, sticky="e", padx=(0, 8))
        ttk.Button(self.header, text="설정", command=lambda: self._show_view("settings")).grid(row=0, column=3, sticky="e")

        self.content = ttk.Frame(self.root, padding=(14, 0, 14, 0))
        self.content.grid(row=1, column=0, sticky="nsew")
        self.content.columnconfigure(0, weight=1)
        self.content.rowconfigure(0, weight=1)

        main_outer, self.main_view = self._create_scrollable_view(self.content)
        settings_outer, self.settings_view = self._create_scrollable_view(self.content)
        self.log_view = ttk.Frame(self.content)
        self.view_frames = {"main": main_outer, "log": self.log_view, "settings": settings_outer}
        for frame in self.view_frames.values():
            frame.grid(row=0, column=0, sticky="nsew")

        self._build_main_view()
        self._build_log_view()
        self._build_settings_view()

        self.nav_bar = ttk.Frame(self.root, padding=(14, 10))
        self.nav_bar.grid(row=2, column=0, sticky="ew")
        for idx in range(3):
            self.nav_bar.columnconfigure(idx, weight=1)
        self.nav_buttons["main"] = ttk.Button(self.nav_bar, text="데이터 정리", command=lambda: self._show_view("main"))
        self.nav_buttons["main"].grid(row=0, column=0, sticky="ew", padx=6)
        self.nav_buttons["log"] = ttk.Button(self.nav_bar, text="로그 기록", command=lambda: self._show_view("log"))
        self.nav_buttons["log"].grid(row=0, column=1, sticky="ew", padx=6)
        self.nav_buttons["settings"] = ttk.Button(self.nav_bar, text="설정", command=lambda: self._show_view("settings"))
        self.nav_buttons["settings"].grid(row=0, column=2, sticky="ew", padx=6)
        self._show_view("main")

    def _create_scrollable_view(self, parent):
        outer = ttk.Frame(parent)
        outer.columnconfigure(0, weight=1)
        outer.rowconfigure(0, weight=1)
        canvas = tk.Canvas(outer, highlightthickness=0, borderwidth=0)
        scrollbar = ttk.Scrollbar(outer, orient="vertical", command=canvas.yview)
        inner = ttk.Frame(canvas)
        inner.bind("<Configure>", lambda _e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")
        canvas.create_window((0, 0), window=inner, anchor="nw", width=1080)
        self.scroll_canvases[str(id(outer))] = canvas
        return outer, inner

    def _build_main_view(self) -> None:
        self.main_view.columnconfigure(0, weight=1)
        source_card = self._make_card(self.main_view, "데이터 소스 설정", 0)
        self._add_source_block(source_card, "이미지 통합 폴더", self.image_root_var, self._choose_image_root)
        self._add_source_block(source_card, "측정 데이터 상위 폴더", self.data_root_var, self._choose_data_root)
        self._add_spin_block(source_card, "비검정 판정 임계값", self.threshold_var)
        self._add_spin_block(source_card, "크롭 패딩(px)", self.padding_var)

        status_card = self._make_card(self.main_view, "현재 상태", 1)
        status_row = ttk.Frame(status_card, style="Card.TFrame")
        status_row.pack(fill="x", pady=10)
        status_box = ttk.Frame(status_row, padding=10, style="Panel.TFrame")
        status_box.pack(side="left", fill="both", expand=True, padx=(0, 10))
        self.status_badge = tk.Label(status_box, text="⌛", font=("Malgun Gothic", 24, "bold"))
        self.status_badge.pack()
        ttk.Label(status_box, textvariable=self.session_note_var, style="StatusNote.TLabel").pack()
        self.run_button = ttk.Button(status_row, text="실행", style="BigAction.TButton", command=self.start_run)
        self.run_button.pack(side="left", fill="both", expand=True, padx=5)
        self.stop_button = ttk.Button(status_row, text="중지", style="BigStop.TButton", command=self.stop_run, state="disabled")
        self.stop_button.pack(side="left", fill="both", expand=True)
        self.progress = ttk.Progressbar(status_card, mode="indeterminate")
        self.progress.pack(fill="x", pady=10)

        result_card = self._make_card(self.main_view, "결과 파일", 2)
        ttk.Label(result_card, textvariable=self.result_var, style="Field.TLabel", wraplength=900, justify="left").pack(anchor="w", pady=(0, 8))
        actions = ttk.Frame(result_card, style="Card.TFrame")
        actions.pack(fill="x")
        ttk.Button(actions, text="메인 엑셀 열기", command=self._open_main_excel).pack(side="left", padx=(0, 6))
        ttk.Button(actions, text="BU 분석 열기", command=self._open_bu_excel).pack(side="left", padx=(0, 6))
        ttk.Button(actions, text="결과 폴더", command=self._open_result_folder).pack(side="left")

    def _build_log_view(self) -> None:
        self.log_view.columnconfigure(0, weight=1)
        self.log_view.rowconfigure(1, weight=1)
        top_bar = ttk.Frame(self.log_view, padding=10)
        top_bar.grid(row=0, column=0, sticky="ew")
        ttk.Label(top_bar, textvariable=self.log_path_var).pack(side="left")
        ttk.Button(top_bar, text="로그 지우기", command=self._clear_log).pack(side="right")
        ttk.Button(top_bar, text="로그 열기", command=lambda: self._open_path(self.log_path)).pack(side="right", padx=(0, 6))
        self.log_text = tk.Text(self.log_view, wrap="word", relief="flat", padx=15, pady=15)
        self.log_text.grid(row=1, column=0, sticky="nsew")
        self.log_text.configure(state="disabled")

    def _build_settings_view(self) -> None:
        self.settings_view.columnconfigure(0, weight=1)
        card = self._make_card(self.settings_view, "일반 설정", 0)
        ttk.Label(card, text="테마", style="Field.TLabel").pack(anchor="w", pady=(10, 0))
        theme_combo = ttk.Combobox(card, textvariable=self.theme_name_var, values=["light", "dark"], state="readonly")
        theme_combo.pack(anchor="w", pady=5)
        theme_combo.bind("<<ComboboxSelected>>", lambda _e: self._apply_theme())
        ttk.Label(card, text="완료 후 자동 열기", style="Field.TLabel").pack(anchor="w", pady=(10, 0))
        auto_combo = ttk.Combobox(card, textvariable=self.auto_open_var, values=["none", "excel", "folder", "both"], state="readonly")
        auto_combo.pack(anchor="w", pady=5)
        ttk.Checkbutton(card, text="최근 사용 경로 기억", variable=self.remember_paths_var).pack(anchor="w", pady=10)
        ttk.Button(card, text="설정 저장", command=self._save_settings).pack(anchor="e")

    def _make_card(self, parent, title: str, row: int):
        card = ttk.Frame(parent, padding=18, style="Card.TFrame")
        card.grid(row=row, column=0, sticky="ew", pady=(0, 12))
        ttk.Label(card, text=title, style="SectionTitle.TLabel").pack(anchor="w", pady=(0, 10))
        return card

    def _add_source_block(self, parent, title: str, variable: tk.StringVar, command) -> None:
        block = ttk.Frame(parent, style="Card.TFrame")
        block.pack(fill="x", pady=5)
        ttk.Label(block, text=title, style="Field.TLabel").pack(anchor="w")
        row_frame = ttk.Frame(block, style="Card.TFrame")
        row_frame.pack(fill="x", pady=5)
        ttk.Entry(row_frame, textvariable=variable).pack(side="left", fill="x", expand=True, padx=(0, 10))
        ttk.Button(row_frame, text="찾아보기", command=command).pack(side="right")

    def _add_spin_block(self, parent, title: str, variable: tk.StringVar) -> None:
        block = ttk.Frame(parent, style="Card.TFrame")
        block.pack(fill="x", pady=5)
        ttk.Label(block, text=title, style="Field.TLabel").pack(side="left")
        ttk.Entry(block, textvariable=variable, width=15).pack(side="left", padx=15)

    def _apply_theme(self) -> None:
        theme = THEMES.get(self.theme_name_var.get(), THEMES["light"])
        self.root.configure(bg=theme["app_bg"])
        self.style.configure("TFrame", background=theme["app_bg"])
        self.style.configure("Card.TFrame", background=theme["card_bg"])
        self.style.configure("Panel.TFrame", background=theme["panel_bg"])
        self.style.configure("SectionTitle.TLabel", background=theme["card_bg"], foreground=theme["text"], font=("Malgun Gothic", 14, "bold"))
        self.style.configure("Field.TLabel", background=theme["card_bg"], foreground=theme["text"], font=("Malgun Gothic", 10, "bold"))
        self.style.configure("StatusNote.TLabel", background=theme["panel_bg"], foreground=theme["subtext"])
        self.style.configure("Nav.TFrame", background=theme["nav_bg"])
        self.header.configure(style="Nav.TFrame")
        self.nav_bar.configure(style="Nav.TFrame")
        if self.log_text is not None:
            self.log_text.configure(bg=theme["log_bg"], fg=theme["log_fg"], insertbackground=theme["log_fg"])
        self._refresh_status_display()

    def _show_view(self, name: str) -> None:
        self.current_view = name
        self.view_frames[name].tkraise()
        self._apply_theme()

    def _choose_image_root(self) -> None:
        path = filedialog.askdirectory()
        if path:
            self.image_root_var.set(path)
            self.root.update_idletasks()

    def _choose_data_root(self) -> None:
        path = filedialog.askdirectory()
        if path:
            self.data_root_var.set(path)
            self.root.update_idletasks()

    def _on_mousewheel(self, event) -> None:
        canvas = self.scroll_canvases.get(str(id(self.view_frames.get(self.current_view))))
        if canvas is not None:
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

    def _poll_log_queue(self) -> None:
        while True:
            try:
                msg = self.log_queue.get_nowait()
            except queue.Empty:
                break
            if self.log_text is not None:
                self.log_text.configure(state="normal")
                self.log_text.insert("end", msg)
                self.log_text.see("end")
                self.log_text.configure(state="disabled")
        self.root.after(100, self._poll_log_queue)

    def _refresh_status_display(self) -> None:
        status = self.status_var.get()
        if self.status_badge is None:
            return
        if "실행" in status:
            self.status_badge.configure(text="▶", fg="#2563eb")
        elif "완료" in status:
            self.status_badge.configure(text="✓", fg="#15803d")
        elif "오류" in status:
            self.status_badge.configure(text="!", fg="#dc2626")
        elif "중지" in status:
            self.status_badge.configure(text="■", fg="#d97706")
        else:
            self.status_badge.configure(text="⌛", fg="#64748b")

    def start_run(self) -> None:
        if self.worker and self.worker.is_alive():
            return
        image_root = Path(self.image_root_var.get().strip())
        data_root = Path(self.data_root_var.get().strip())
        if not image_root.exists() or not data_root.exists():
            messagebox.showerror("오류", "폴더 경로를 확인하세요.")
            return
        self.cancel_requested.clear()
        self.status_var.set("실행 중")
        self.session_note_var.set("작업 진행 중")
        self.run_button.configure(state="disabled")
        self.stop_button.configure(state="normal")
        self.progress.start(10)
        self.worker = threading.Thread(target=self._run_worker, args=(image_root, data_root), daemon=True)
        self.worker.start()
        self._refresh_status_display()

    def _run_worker(self, image_root: Path, data_root: Path) -> None:
        writer = QueueWriter(self.log_queue, self.log_path)
        try:
            with redirect_stdout(writer), redirect_stderr(writer):
                threshold = int(self.threshold_var.get() or 12)
                padding = int(self.padding_var.get() or 8)
                result = self.run_pipeline_func(image_root, data_root, threshold, padding, cancel_check=self.cancel_requested.is_set)
                self.root.after(0, self._on_success, result)
        except self.pipeline_cancelled_exc:
            self.root.after(0, self._on_cancelled)
        except Exception as exc:
            self.root.after(0, self._on_failure, str(exc))

    def _on_success(self, result: dict) -> None:
        self.latest_result = result
        self.progress.stop()
        self.run_button.configure(state="normal")
        self.stop_button.configure(state="disabled")
        self.status_var.set("완료")
        self.session_note_var.set("DATA 정리 완료")
        self.result_var.set(f"메인 엑셀: {result.get('excel_path', '')}\nBU 분석 엑셀: {result.get('bu_analysis_excel_path', '')}")
        self._refresh_status_display()
        messagebox.showinfo("완료", "데이터 정리가 완료되었습니다.")
        self._handle_auto_open()

    def _on_cancelled(self) -> None:
        self.progress.stop()
        self.run_button.configure(state="normal")
        self.stop_button.configure(state="disabled")
        self.status_var.set("중지됨")
        self.session_note_var.set("사용자 요청으로 중지")
        self._refresh_status_display()
        messagebox.showinfo("중지", "작업이 중지되었습니다.")

    def _on_failure(self, err: str) -> None:
        self.progress.stop()
        self.run_button.configure(state="normal")
        self.stop_button.configure(state="disabled")
        self.status_var.set("오류")
        self.session_note_var.set("실행 중 오류 발생")
        self._refresh_status_display()
        messagebox.showerror("오류", err)

    def _handle_auto_open(self) -> None:
        if not self.latest_result:
            return
        mode = self.auto_open_var.get()
        excel_path = self.latest_result.get("excel_path")
        if not excel_path:
            return
        result_dir = Path(excel_path).parent
        if mode == "excel":
            self._open_path(Path(excel_path))
        elif mode == "folder":
            self._open_path(result_dir)
        elif mode == "both":
            self._open_path(Path(excel_path))
            self._open_path(result_dir)

    def stop_run(self) -> None:
        self.cancel_requested.set()
        self.status_var.set("중지 요청됨")
        self.session_note_var.set("현재 단계 후 안전 중지")
        self._refresh_status_display()

    def _clear_log(self) -> None:
        if self.log_text is None:
            return
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.configure(state="disabled")

    def _open_path(self, path: Path) -> None:
        try:
            os.startfile(str(path))
        except OSError as exc:
            messagebox.showerror("열기 실패", str(exc))

    def _open_main_excel(self) -> None:
        if self.latest_result and self.latest_result.get("excel_path"):
            self._open_path(Path(self.latest_result["excel_path"]))

    def _open_bu_excel(self) -> None:
        if self.latest_result and self.latest_result.get("bu_analysis_excel_path"):
            self._open_path(Path(self.latest_result["bu_analysis_excel_path"]))

    def _open_result_folder(self) -> None:
        if self.latest_result and self.latest_result.get("excel_path"):
            self._open_path(Path(self.latest_result["excel_path"]).parent)


def launch_app(config: GuiConfig, run_pipeline_func, pipeline_cancelled_exc) -> None:
    threading.Thread(target=show_splash, args=(config,), daemon=True).start()
    root = tk.Tk()
    BUOrganizeApp(root, config, run_pipeline_func, pipeline_cancelled_exc)
    root.mainloop()
