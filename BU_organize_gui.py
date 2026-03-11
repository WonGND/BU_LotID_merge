import io
import os
import queue
import threading
import tkinter as tk
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from BU_organize_one_click import run_pipeline

APP_NAME = "TOVIS_BU_DATA_정리_v0.1"
ICON_PATH = Path(__file__).with_name("tovis_bu_data.ico")


class QueueWriter(io.TextIOBase):
    def __init__(self, log_queue: queue.Queue):
        self.log_queue = log_queue

    def write(self, s: str) -> int:
        if s:
            self.log_queue.put(s)
        return len(s)

    def flush(self) -> None:
        return


class BUOrganizeApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title(APP_NAME)
        self.root.geometry("980x720")
        self.root.minsize(860, 640)
        self._apply_icon()

        self.log_queue: queue.Queue[str] = queue.Queue()
        self.worker: threading.Thread | None = None

        self.image_root_var = tk.StringVar()
        self.data_root_var = tk.StringVar()
        self.threshold_var = tk.StringVar(value="12")
        self.padding_var = tk.StringVar(value="8")
        self.status_var = tk.StringVar(value="대기 중")
        self.result_var = tk.StringVar(value="결과 파일 경로가 여기에 표시됩니다.")

        self._build_ui()
        self._poll_log_queue()

    def _apply_icon(self) -> None:
        if ICON_PATH.exists():
            try:
                self.root.iconbitmap(str(ICON_PATH))
            except tk.TclError:
                pass

    def _build_ui(self) -> None:
        self.root.configure(bg="#f3f0e8")

        style = ttk.Style()
        style.theme_use("clam")
        style.configure("App.TFrame", background="#f3f0e8")
        style.configure("Card.TFrame", background="#fffaf0", relief="flat")
        style.configure("Title.TLabel", background="#f3f0e8", foreground="#1f2937", font=("Malgun Gothic", 18, "bold"))
        style.configure("Body.TLabel", background="#fffaf0", foreground="#374151", font=("Malgun Gothic", 10))
        style.configure("Field.TLabel", background="#fffaf0", foreground="#111827", font=("Malgun Gothic", 10, "bold"))
        style.configure("Run.TButton", font=("Malgun Gothic", 11, "bold"))
        style.configure("Path.TButton", font=("Malgun Gothic", 10))

        outer = ttk.Frame(self.root, style="App.TFrame", padding=18)
        outer.pack(fill="both", expand=True)

        title = ttk.Label(outer, text=APP_NAME, style="Title.TLabel")
        title.pack(anchor="w")
        subtitle = ttk.Label(
            outer,
            text="이미지 병합, 측정값 최신화, 크롭, 엑셀 작성까지 한 번에 실행합니다.",
            style="Body.TLabel",
        )
        subtitle.pack(anchor="w", pady=(4, 14))

        card = ttk.Frame(outer, style="Card.TFrame", padding=18)
        card.pack(fill="x")

        self._add_path_row(card, 0, "이미지 통합 폴더", self.image_root_var, self._choose_image_root)
        self._add_path_row(card, 1, "측정 데이터 상위 폴더", self.data_root_var, self._choose_data_root)
        self._add_entry_row(card, 2, "비검정 판정 임계값", self.threshold_var)
        self._add_entry_row(card, 3, "크롭 패딩(px)", self.padding_var)

        action_row = ttk.Frame(card, style="Card.TFrame")
        action_row.grid(row=4, column=0, columnspan=3, sticky="ew", pady=(12, 0))
        action_row.columnconfigure(0, weight=1)

        self.run_button = ttk.Button(action_row, text="실행", style="Run.TButton", command=self.start_run)
        self.run_button.grid(row=0, column=0, sticky="ew")

        self.progress = ttk.Progressbar(action_row, mode="indeterminate")
        self.progress.grid(row=1, column=0, sticky="ew", pady=(12, 0))

        status_card = ttk.Frame(outer, style="Card.TFrame", padding=18)
        status_card.pack(fill="x", pady=(14, 0))
        ttk.Label(status_card, text="상태", style="Field.TLabel").pack(anchor="w")
        ttk.Label(status_card, textvariable=self.status_var, style="Body.TLabel").pack(anchor="w", pady=(6, 0))
        ttk.Label(status_card, text="결과 엑셀", style="Field.TLabel").pack(anchor="w", pady=(12, 0))
        ttk.Label(status_card, textvariable=self.result_var, style="Body.TLabel", wraplength=900).pack(anchor="w", pady=(6, 0))

        log_card = ttk.Frame(outer, style="Card.TFrame", padding=18)
        log_card.pack(fill="both", expand=True, pady=(14, 0))
        ttk.Label(log_card, text="실행 로그", style="Field.TLabel").pack(anchor="w")

        self.log_text = tk.Text(
            log_card,
            height=18,
            wrap="word",
            bg="#111827",
            fg="#e5e7eb",
            insertbackground="#e5e7eb",
            font=("Consolas", 10),
            relief="flat",
            padx=12,
            pady=12,
        )
        self.log_text.pack(fill="both", expand=True, pady=(10, 0))
        self.log_text.configure(state="disabled")

    def _add_path_row(self, parent, row: int, label: str, variable: tk.StringVar, command) -> None:
        ttk.Label(parent, text=label, style="Field.TLabel").grid(row=row, column=0, sticky="w", pady=6)
        entry = ttk.Entry(parent, textvariable=variable, width=90)
        entry.grid(row=row, column=1, sticky="ew", padx=(12, 10), pady=6)
        ttk.Button(parent, text="찾아보기", style="Path.TButton", command=command).grid(row=row, column=2, sticky="ew", pady=6)
        parent.columnconfigure(1, weight=1)

    def _add_entry_row(self, parent, row: int, label: str, variable: tk.StringVar) -> None:
        ttk.Label(parent, text=label, style="Field.TLabel").grid(row=row, column=0, sticky="w", pady=6)
        ttk.Entry(parent, textvariable=variable, width=18).grid(row=row, column=1, sticky="w", padx=(12, 10), pady=6)

    def _choose_image_root(self) -> None:
        path = filedialog.askdirectory(title="이미지 통합 폴더 선택")
        if path:
            self.image_root_var.set(path)

    def _choose_data_root(self) -> None:
        path = filedialog.askdirectory(title="측정 데이터 상위 폴더 선택")
        if path:
            self.data_root_var.set(path)

    def _append_log(self, text: str) -> None:
        self.log_text.configure(state="normal")
        self.log_text.insert("end", text)
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _poll_log_queue(self) -> None:
        while True:
            try:
                message = self.log_queue.get_nowait()
            except queue.Empty:
                break
            self._append_log(message)
        self.root.after(120, self._poll_log_queue)

    def start_run(self) -> None:
        if self.worker and self.worker.is_alive():
            messagebox.showinfo("실행 중", "현재 작업이 진행 중입니다.")
            return

        image_root = Path(self.image_root_var.get().strip())
        data_root = Path(self.data_root_var.get().strip())

        if not image_root.exists() or not image_root.is_dir():
            messagebox.showerror("입력 오류", "이미지 통합 폴더 경로를 확인하세요.")
            return
        if not data_root.exists() or not data_root.is_dir():
            messagebox.showerror("입력 오류", "측정 데이터 상위 폴더 경로를 확인하세요.")
            return

        try:
            threshold = int(self.threshold_var.get().strip())
            padding = int(self.padding_var.get().strip())
        except ValueError:
            messagebox.showerror("입력 오류", "임계값과 패딩은 숫자로 입력해야 합니다.")
            return

        self.status_var.set("실행 중")
        self.result_var.set("작업이 진행 중입니다.")
        self.run_button.configure(state="disabled")
        self.progress.start(10)
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.configure(state="disabled")

        self.worker = threading.Thread(
            target=self._run_worker,
            args=(image_root, data_root, threshold, padding),
            daemon=True,
        )
        self.worker.start()

    def _run_worker(self, image_root: Path, data_root: Path, threshold: int, padding: int) -> None:
        writer = QueueWriter(self.log_queue)
        try:
            with redirect_stdout(writer), redirect_stderr(writer):
                result = run_pipeline(image_root, data_root, threshold, padding)
            self.root.after(0, self._on_success, result)
        except Exception as exc:
            self.root.after(0, self._on_failure, str(exc))

    def _on_success(self, result: dict) -> None:
        self.progress.stop()
        self.run_button.configure(state="normal")
        self.status_var.set("완료")
        self.result_var.set(str(result["excel_path"]))
        self._append_log(f"\n완료: {result['excel_path']}\n")
        should_open = messagebox.askyesno("완료", "작업이 완료되었습니다.\n결과 엑셀 파일을 바로 열까요?")
        if should_open:
            self._open_result(result["excel_path"])

    def _on_failure(self, error_message: str) -> None:
        self.progress.stop()
        self.run_button.configure(state="normal")
        self.status_var.set("오류 발생")
        self.result_var.set("오류로 인해 결과 파일이 생성되지 않았습니다.")
        self._append_log(f"\n오류: {error_message}\n")
        messagebox.showerror("실행 오류", error_message)

    def _open_result(self, result_path: str | Path) -> None:
        try:
            os.startfile(str(result_path))
        except OSError as exc:
            messagebox.showerror("열기 실패", f"결과 파일을 열지 못했습니다.\n{exc}")


def main() -> None:
    root = tk.Tk()
    app = BUOrganizeApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
