import csv
import re
import shutil
from datetime import datetime
from pathlib import Path

from openpyxl.chart import BarChart, LineChart, PieChart, Reference
from openpyxl.chart.label import DataLabelList
from openpyxl.formatting.rule import CellIsRule, FormulaRule
from openpyxl.styles import Font, PatternFill
from openpyxl import Workbook
from openpyxl.drawing.image import Image as XLImage
from PIL import Image

# 처리 대상 이미지 확장자
ALLOWED_EXTENSIONS = (".png", ".jpg", ".jpeg", ".bmp", ".tif", ".tiff", ".webp")
# 파일명에서 LotID/종류(BU, WU)를 뽑기 위한 패턴
LOT_PATTERN = re.compile(r"^(?P<lotid>.+)_(?P<kind>BU|WU)_\d+$", re.IGNORECASE)
DATA_FILE_PATTERN = "LMK6DataLog.csv"
BU_SPEC_MIN = 50.0
WU_SPEC_MIN = 80.0


class PipelineCancelled(Exception):
    pass


def print_progress(label: str, current: int, total: int, done: bool = False) -> None:
    # 진행률 표시 공통 함수
    if total <= 0:
        return
    percent = (current / total) * 100
    print(f"{label}: {current}/{total} ({percent:5.1f}%)", flush=True)


def ensure_not_cancelled(cancel_check=None) -> None:
    if cancel_check and cancel_check():
        raise PipelineCancelled("사용자 요청으로 작업이 중지되었습니다.")


def unique_folder_path(base_dir: Path, folder_name: str) -> Path:
    # 동일 폴더명이 이미 있으면 _1, _2 ... 를 붙여 새 경로를 만든다.
    candidate = base_dir / folder_name
    if not candidate.exists():
        return candidate

    idx = 1
    while True:
        candidate = base_dir / f"{folder_name}_{idx}"
        if not candidate.exists():
            return candidate
        idx += 1


def unique_file_path(path: Path) -> Path:
    # 동일 파일명이 이미 있으면 파일명 뒤에 _1, _2 ... 를 붙여 저장 경로를 만든다.
    if not path.exists():
        return path

    parent = path.parent
    stem = path.stem
    suffix = path.suffix
    idx = 1
    while True:
        candidate = parent / f"{stem}_{idx}{suffix}"
        if not candidate.exists():
            return candidate
        idx += 1


def ask_int(prompt: str, default: int) -> int:
    # 숫자 입력(엔터면 기본값 사용)
    raw = input(f"{prompt} (기본값 {default}): ").strip().replace('"', "")
    if not raw:
        return default
    return int(raw)


def ask_path(prompt: str) -> Path:
    return Path(input(prompt).strip().replace('"', ""))


def folder_time_key(path: Path) -> tuple[float, float]:
    # 최신 폴더 비교 기준: 생성시각 우선, 동일하면 수정시각
    stat = path.stat()
    return (stat.st_ctime, stat.st_mtime)


def is_lotid_folder(path: Path) -> bool:
    # LotID 폴더 판정 규칙: 이미지 파일이 1개 이상 있는 디렉터리
    if not path.is_dir():
        return False
    image_count = 0
    for child in path.iterdir():
        if child.is_file() and child.suffix.lower() in ALLOWED_EXTENSIONS:
            image_count += 1
    return image_count >= 1


def format_ts(ts: float) -> str:
    # CSV 가독성을 위해 타임스탬프를 날짜 문자열로 변환
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def parse_lmk_time(value: str) -> datetime:
    # LMK 로그 시간 형식: 2026.02.24 09:30:51
    return datetime.strptime(value.strip(), "%Y.%m.%d %H:%M:%S")


def format_measurement_value(value: str) -> str:
    # 엑셀에는 핵심 수치만 간단히 넣는다.
    if value is None:
        return ""
    return str(value).strip()


def to_float(value) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def iter_csv_dict_rows(csv_path: Path):
    # CSV 인코딩이 파일마다 다를 수 있어서 순차적으로 시도한다.
    encodings = ("utf-8-sig", "cp949", "euc-kr", "utf-8")
    last_error = None

    for encoding in encodings:
        try:
            with csv_path.open("r", encoding=encoding, newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            return rows
        except UnicodeDecodeError as exc:
            last_error = exc

    raise UnicodeDecodeError(
        getattr(last_error, "encoding", "unknown"),
        getattr(last_error, "object", b""),
        getattr(last_error, "start", 0),
        getattr(last_error, "end", 1),
        f"{csv_path} 파일 인코딩을 읽지 못함",
    )


def collect_latest_measurements(data_root: Path, cancel_check=None) -> tuple[dict[str, dict], list[dict]]:
    # 여러 LMK6DataLog.csv를 재귀 탐색해 Panel_ID 기준 최신 행만 남긴다.
    csv_files = sorted(data_root.rglob(DATA_FILE_PATTERN))
    total_files = len(csv_files)
    print(f"\n[4/7] 측정 CSV 스캔 시작 (대상 파일: {total_files}개)")

    latest_by_lotid: dict[str, dict] = {}
    rows_for_detail: list[dict] = []
    if total_files == 0:
        print("  측정 CSV를 찾지 못해서 데이터 입력은 공란으로 둔다.")
        return latest_by_lotid, rows_for_detail

    for file_idx, csv_path in enumerate(csv_files, start=1):
        ensure_not_cancelled(cancel_check)
        if file_idx == 1 or file_idx % 10 == 0 or file_idx == total_files:
            print_progress("  CSV 스캔 진행", file_idx, total_files, done=(file_idx == total_files))

        for row in iter_csv_dict_rows(csv_path):
            ensure_not_cancelled(cancel_check)
            lot_id = (row.get("Panel_ID") or "").strip()
            if not lot_id:
                continue

            measured_at_raw = row.get("Time", "")
            measured_at = parse_lmk_time(measured_at_raw)
            record = {
                "lot_id": lot_id,
                "judge": (row.get("Judge") or "").strip(),
                "black_uniformity": format_measurement_value(row.get("Black_Uniformity")),
                "white_uniformity": format_measurement_value(row.get("White_Uniformity")),
                "time_raw": measured_at_raw,
                "time_obj": measured_at,
                "source_file": str(csv_path),
            }

            current = latest_by_lotid.get(lot_id)
            is_latest = current is None or measured_at >= current["time_obj"]
            if is_latest:
                latest_by_lotid[lot_id] = record

            rows_for_detail.append(
                {
                    "lot_id": lot_id,
                    "judge": record["judge"],
                    "black_uniformity": record["black_uniformity"],
                    "white_uniformity": record["white_uniformity"],
                    "time_raw": measured_at_raw,
                    "source_file": str(csv_path),
                    "selected_latest_final": "FALSE",
                }
            )

    latest_signatures = {
        (value["lot_id"], value["time_raw"], value["source_file"]) for value in latest_by_lotid.values()
    }
    for row in rows_for_detail:
        row["selected_latest_final"] = (
            "TRUE"
            if (row["lot_id"], row["time_raw"], row["source_file"]) in latest_signatures
            else "FALSE"
        )

    return latest_by_lotid, rows_for_detail


def build_metric_summary(latest_measurements: dict[str, dict], key: str, spec_min: float) -> dict:
    values = [to_float(row.get(key)) for row in latest_measurements.values()]
    values = [v for v in values if v is not None]
    if not values:
        return {
            "count": 0,
            "pass_count": 0,
            "fail_count": 0,
            "min": None,
            "avg": None,
            "max": None,
            "median": None,
            "sorted_values": [],
        }

    sorted_values = sorted(values)
    count = len(sorted_values)
    mid = count // 2
    if count % 2 == 0:
        median = (sorted_values[mid - 1] + sorted_values[mid]) / 2
    else:
        median = sorted_values[mid]

    pass_count = sum(1 for value in sorted_values if value >= spec_min)
    return {
        "count": count,
        "pass_count": pass_count,
        "fail_count": count - pass_count,
        "min": min(sorted_values),
        "avg": sum(sorted_values) / count,
        "max": max(sorted_values),
        "median": median,
        "sorted_values": sorted_values,
    }


def build_distribution(values: list[float], bins: list[tuple[str, float, float]]) -> list[tuple[str, int]]:
    counts: list[tuple[str, int]] = []
    for label, lower, upper in bins:
        count = sum(1 for value in values if lower <= value < upper)
        counts.append((label, count))
    return counts


def add_visualization_sheet(wb: Workbook, latest_measurements: dict[str, dict]) -> None:
    ws = wb.create_sheet("시각화")
    ws["A1"] = "BU/WU 측정 대시보드"
    ws["A1"].font = Font(size=16, bold=True)
    ws["A3"] = "지표"
    ws["B3"] = "Spec Min"
    ws["C3"] = "Count"
    ws["D3"] = "Pass"
    ws["E3"] = "Fail"
    ws["F3"] = "Min"
    ws["G3"] = "Avg"
    ws["H3"] = "Median"
    ws["I3"] = "Max"

    header_fill = PatternFill("solid", fgColor="1F2937")
    header_font = Font(color="FFFFFF", bold=True)
    for cell in ws["3:3"]:
        cell.fill = header_fill
        cell.font = header_font

    bu_summary = build_metric_summary(latest_measurements, "black_uniformity", BU_SPEC_MIN)
    wu_summary = build_metric_summary(latest_measurements, "white_uniformity", WU_SPEC_MIN)

    metric_rows = [
        ("Black Uniformity", BU_SPEC_MIN, bu_summary),
        ("White Uniformity", WU_SPEC_MIN, wu_summary),
    ]
    for row_idx, (label, spec_min, summary) in enumerate(metric_rows, start=4):
        ws.cell(row=row_idx, column=1, value=label)
        ws.cell(row=row_idx, column=2, value=spec_min)
        ws.cell(row=row_idx, column=3, value=summary["count"])
        ws.cell(row=row_idx, column=4, value=summary["pass_count"])
        ws.cell(row=row_idx, column=5, value=summary["fail_count"])
        ws.cell(row=row_idx, column=6, value=summary["min"])
        ws.cell(row=row_idx, column=7, value=summary["avg"])
        ws.cell(row=row_idx, column=8, value=summary["median"])
        ws.cell(row=row_idx, column=9, value=summary["max"])

    # 정렬 추세 차트용 데이터
    ws["K2"] = "BU_index"
    ws["L2"] = "BU_value"
    ws["M2"] = "BU_spec"
    for idx, value in enumerate(bu_summary["sorted_values"], start=3):
        ws.cell(row=idx, column=11, value=idx - 2)
        ws.cell(row=idx, column=12, value=value)
        ws.cell(row=idx, column=13, value=BU_SPEC_MIN)

    ws["O2"] = "WU_index"
    ws["P2"] = "WU_value"
    ws["Q2"] = "WU_spec"
    for idx, value in enumerate(wu_summary["sorted_values"], start=3):
        ws.cell(row=idx, column=15, value=idx - 2)
        ws.cell(row=idx, column=16, value=value)
        ws.cell(row=idx, column=17, value=WU_SPEC_MIN)

    bu_line = LineChart()
    bu_line.title = "BU 분포 추세"
    bu_line.y_axis.title = "Black Uniformity"
    bu_line.x_axis.title = "정렬 순서"
    bu_data = Reference(ws, min_col=12, max_col=13, min_row=2, max_row=max(3, len(bu_summary["sorted_values"]) + 2))
    bu_cats = Reference(ws, min_col=11, min_row=3, max_row=max(3, len(bu_summary["sorted_values"]) + 2))
    bu_line.add_data(bu_data, titles_from_data=True)
    bu_line.set_categories(bu_cats)
    bu_line.height = 7
    bu_line.width = 13
    ws.add_chart(bu_line, "A8")

    wu_line = LineChart()
    wu_line.title = "WU 분포 추세"
    wu_line.y_axis.title = "White Uniformity"
    wu_line.x_axis.title = "정렬 순서"
    wu_data = Reference(ws, min_col=16, max_col=17, min_row=2, max_row=max(3, len(wu_summary["sorted_values"]) + 2))
    wu_cats = Reference(ws, min_col=15, min_row=3, max_row=max(3, len(wu_summary["sorted_values"]) + 2))
    wu_line.add_data(wu_data, titles_from_data=True)
    wu_line.set_categories(wu_cats)
    wu_line.height = 7
    wu_line.width = 13
    ws.add_chart(wu_line, "N8")

    # Pass/Fail 파이 차트
    ws["A24"] = "Metric"
    ws["B24"] = "Pass"
    ws["C24"] = "Fail"
    ws["A25"] = "BU"
    ws["B25"] = bu_summary["pass_count"]
    ws["C25"] = bu_summary["fail_count"]
    ws["A26"] = "WU"
    ws["B26"] = wu_summary["pass_count"]
    ws["C26"] = wu_summary["fail_count"]

    bu_pie = PieChart()
    bu_pie.title = "BU Pass/Fail"
    bu_pie.add_data(Reference(ws, min_col=2, max_col=3, min_row=25, max_row=25), from_rows=True)
    bu_pie.set_categories(Reference(ws, min_col=2, max_col=3, min_row=24, max_row=24))
    bu_pie.height = 6
    bu_pie.width = 8
    bu_pie.dataLabels = DataLabelList()
    bu_pie.dataLabels.showPercent = True
    ws.add_chart(bu_pie, "A28")

    wu_pie = PieChart()
    wu_pie.title = "WU Pass/Fail"
    wu_pie.add_data(Reference(ws, min_col=2, max_col=3, min_row=26, max_row=26), from_rows=True)
    wu_pie.set_categories(Reference(ws, min_col=2, max_col=3, min_row=24, max_row=24))
    wu_pie.height = 6
    wu_pie.width = 8
    wu_pie.dataLabels = DataLabelList()
    wu_pie.dataLabels.showPercent = True
    ws.add_chart(wu_pie, "J28")

    # Spec 기준 중심 버킷 분포
    bu_bins = [
        ("<40", 0, 40),
        ("40-45", 40, 45),
        ("45-50", 45, 50),
        ("50-55", 50, 55),
        ("55-60", 55, 60),
        ("60+", 60, 9999),
    ]
    wu_bins = [
        ("<70", 0, 70),
        ("70-75", 70, 75),
        ("75-80", 75, 80),
        ("80-85", 80, 85),
        ("85-90", 85, 90),
        ("90+", 90, 9999),
    ]
    bu_distribution = build_distribution(bu_summary["sorted_values"], bu_bins)
    wu_distribution = build_distribution(wu_summary["sorted_values"], wu_bins)

    ws["S2"] = "BU_bucket"
    ws["T2"] = "BU_count"
    for idx, (label, count) in enumerate(bu_distribution, start=3):
        ws.cell(row=idx, column=19, value=label)
        ws.cell(row=idx, column=20, value=count)

    ws["V2"] = "WU_bucket"
    ws["W2"] = "WU_count"
    for idx, (label, count) in enumerate(wu_distribution, start=3):
        ws.cell(row=idx, column=22, value=label)
        ws.cell(row=idx, column=23, value=count)

    bu_bar = BarChart()
    bu_bar.title = "BU 분포 구간"
    bu_bar.y_axis.title = "Count"
    bu_bar.x_axis.title = "Range"
    bu_bar.add_data(Reference(ws, min_col=20, min_row=2, max_row=2 + len(bu_distribution)), titles_from_data=True)
    bu_bar.set_categories(Reference(ws, min_col=19, min_row=3, max_row=2 + len(bu_distribution)))
    bu_bar.height = 7
    bu_bar.width = 11
    ws.add_chart(bu_bar, "T8")

    wu_bar = BarChart()
    wu_bar.title = "WU 분포 구간"
    wu_bar.y_axis.title = "Count"
    wu_bar.x_axis.title = "Range"
    wu_bar.add_data(Reference(ws, min_col=23, min_row=2, max_row=2 + len(wu_distribution)), titles_from_data=True)
    wu_bar.set_categories(Reference(ws, min_col=22, min_row=3, max_row=2 + len(wu_distribution)))
    wu_bar.height = 7
    wu_bar.width = 11
    ws.add_chart(wu_bar, "T28")

    ws.column_dimensions["A"].width = 22
    for col in ("B", "C", "D", "E", "F", "G", "H", "I"):
        ws.column_dimensions[col].width = 12


def collect_latest_lotid_folders(integrated_root: Path, cancel_check=None) -> tuple[dict[str, Path], list[dict]]:
    # 전체 폴더를 훑어서 LotID별 최신 폴더 1개만 남긴다.
    latest_by_lotid: dict[str, Path] = {}
    rows: list[dict] = []

    dir_candidates = [p for p in integrated_root.rglob("*") if p.is_dir()]
    total_dirs = len(dir_candidates)
    print(f"\n[1/7] LotID 폴더 스캔 시작 (대상 폴더: {total_dirs}개)")

    for idx, p in enumerate(dir_candidates, start=1):
        ensure_not_cancelled(cancel_check)
        if idx == 1 or idx % 50 == 0 or idx == total_dirs:
            print_progress("  스캔 진행", idx, total_dirs, done=(idx == total_dirs))

        if not is_lotid_folder(p):
            continue

        lot_id = p.name
        current = latest_by_lotid.get(lot_id)
        is_latest = False

        if current is None or folder_time_key(p) > folder_time_key(current):
            latest_by_lotid[lot_id] = p
            is_latest = True

        created_ts = p.stat().st_ctime
        modified_ts = p.stat().st_mtime
        rows.append(
            {
                "lot_id": lot_id,
                "folder_path": str(p),
                "created_time": format_ts(created_ts),
                "modified_time": format_ts(modified_ts),
                "selected_latest_at_scan_time": "TRUE" if is_latest else "FALSE",
            }
        )

    selected_paths = {str(v) for v in latest_by_lotid.values()}
    for row in rows:
        row["selected_latest_final"] = "TRUE" if row["folder_path"] in selected_paths else "FALSE"

    return latest_by_lotid, rows


def copy_latest_folders(latest_by_lotid: dict[str, Path], output_root: Path, cancel_check=None) -> None:
    # 최종 선택된 LotID 폴더만 결과 폴더로 복사
    if output_root.exists():
        shutil.rmtree(output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    items = sorted(latest_by_lotid.items())
    total = len(items)
    print(f"\n[2/7] 최신 LotID 폴더 복사 시작 (대상: {total}개)")

    for idx, (lot_id, src) in enumerate(items, start=1):
        ensure_not_cancelled(cancel_check)
        dst = unique_folder_path(output_root, lot_id)
        shutil.copytree(src, dst)
        if idx == 1 or idx % 20 == 0 or idx == total:
            print_progress("  복사 진행", idx, total, done=(idx == total))


def write_merge_report(rows: list[dict], output_root: Path) -> Path:
    # 병합(merge) 판단 결과를 CSV로 저장
    report_path = output_root / "merge_report.csv"
    fieldnames = [
        "lot_id",
        "folder_path",
        "created_time",
        "modified_time",
        "selected_latest_at_scan_time",
        "selected_latest_final",
    ]
    print("\n[3/7] Merge 리포트 저장")
    with report_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print("  Merge 리포트 저장 완료")
    return report_path


def find_non_black_bbox(img: Image.Image, threshold: int = 12):
    # 검은 배경(저밝기)을 제외한 영역의 최소 사각형(BBox) 검출
    # threshold를 올리면 더 어두운 영역까지 배경으로 간주한다.
    gray = img.convert("L")
    w, h = gray.size
    px = gray.load()

    min_x, min_y = w, h
    max_x, max_y = -1, -1

    for y in range(h):
        for x in range(w):
            if px[x, y] > threshold:
                if x < min_x:
                    min_x = x
                if y < min_y:
                    min_y = y
                if x > max_x:
                    max_x = x
                if y > max_y:
                    max_y = y

    if max_x < min_x or max_y < min_y:
        return None
    return min_x, min_y, max_x + 1, max_y + 1


def add_padding(box, w: int, h: int, pad: int):
    # 잘림 방지를 위해 크롭 박스에 여백(padding) 추가
    left, top, right, bottom = box
    return (
        max(0, left - pad),
        max(0, top - pad),
        min(w, right + pad),
        min(h, bottom + pad),
    )


def parse_lot_kind(stem: str):
    # 파일명에서 LotID와 BU/WU를 파싱
    # 패턴 불일치 시 kind=UNKNOWN으로 처리
    m = LOT_PATTERN.match(stem)
    if not m:
        return stem, "UNKNOWN"
    return m.group("lotid"), m.group("kind").upper()


def crop_images(input_root: Path, output_root: Path, threshold: int, padding: int, cancel_check=None):
    # merge 결과 폴더를 대상으로 자동 크롭 실행
    if output_root.exists():
        shutil.rmtree(output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    image_files = [
        p for p in input_root.rglob("*") if p.is_file() and p.suffix.lower() in ALLOWED_EXTENSIONS
    ]
    total_images = len(image_files)
    print(f"\n[5/7] 이미지 크롭 시작 (대상: {total_images}개)")

    records = []
    for idx, src in enumerate(image_files, start=1):
        ensure_not_cancelled(cancel_check)
        rel = src.relative_to(input_root)
        dst_raw = output_root / rel
        dst = dst_raw
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst = unique_file_path(dst)
        renamed = dst != dst_raw

        lot_id, kind = parse_lot_kind(src.stem)

        try:
            with Image.open(src) as img:
                bbox = find_non_black_bbox(img, threshold=threshold)
                if bbox is None:
                    cropped = img.copy()
                    used_bbox = (0, 0, img.width, img.height)
                    status = "NO_OBJECT_DETECTED"
                else:
                    used_bbox = add_padding(bbox, img.width, img.height, padding)
                    cropped = img.crop(used_bbox)
                    status = "OK"

                if dst.suffix.lower() in (".jpg", ".jpeg") and cropped.mode not in ("RGB", "L"):
                    cropped = cropped.convert("RGB")
                cropped.save(dst)

            records.append(
                {
                    "lot_id": lot_id,
                    "kind": kind,
                    "src": src,
                    "dst": dst,
                    "bbox": used_bbox,
                    "status": status,
                    "renamed_on_save": "TRUE" if renamed else "FALSE",
                }
            )
        except Exception as exc:
            records.append(
                {
                    "lot_id": lot_id,
                    "kind": kind,
                    "src": src,
                    "dst": None,
                    "bbox": None,
                    "status": f"ERROR: {exc}",
                    "renamed_on_save": "FALSE",
                }
            )

        if idx == 1 or idx % 10 == 0 or idx == total_images:
            print_progress("  크롭 진행", idx, total_images, done=(idx == total_images))

    return records


def write_excel(
    records,
    excel_path: Path,
    merge_rows=None,
    latest_measurements=None,
    measurement_rows=None,
    image_width_px: int = 240,
    cancel_check=None,
):
    # 결과 시트: LotID별 BU/WU 이미지 배치 (사용자 입력 컬럼은 공란 유지)
    # 상세 시트: 경로/중복 정보 정리 (이미지 없음)
    wb = Workbook()
    ws = wb.active
    ws.title = "결과"
    ws.append(["LotID", "판정", "BU data", "BU Image", "WU data", "WU Image"])

    total = len(records)
    print(f"\n[6/7] 엑셀 작성 시작 (행: {total}개)")

    grouped: dict[str, dict[str, Path | None]] = {}
    for rec in records:
        lot_id = rec["lot_id"]
        kind = rec["kind"]
        grouped.setdefault(lot_id, {"BU": None, "WU": None})
        if kind in ("BU", "WU") and rec["dst"] is not None and grouped[lot_id][kind] is None:
            grouped[lot_id][kind] = rec["dst"]

    row = 2
    for lot_id in sorted(grouped.keys()):
        ensure_not_cancelled(cancel_check)
        ws.cell(row=row, column=1, value=lot_id)
        measurement = (latest_measurements or {}).get(lot_id, {})
        ws.cell(row=row, column=2, value=measurement.get("judge", ""))
        ws.cell(row=row, column=3, value=measurement.get("black_uniformity", ""))
        ws.cell(row=row, column=5, value=measurement.get("white_uniformity", ""))

        bu_path = grouped[lot_id]["BU"]
        wu_path = grouped[lot_id]["WU"]
        max_img_height = 0

        if bu_path is not None and Path(bu_path).exists():
            bu_img = XLImage(str(bu_path))
            if bu_img.width > image_width_px:
                ratio = image_width_px / bu_img.width
                bu_img.width = int(bu_img.width * ratio)
                bu_img.height = int(bu_img.height * ratio)
            ws.add_image(bu_img, f"D{row}")
            max_img_height = max(max_img_height, bu_img.height)

        if wu_path is not None and Path(wu_path).exists():
            wu_img = XLImage(str(wu_path))
            if wu_img.width > image_width_px:
                ratio = image_width_px / wu_img.width
                wu_img.width = int(wu_img.width * ratio)
                wu_img.height = int(wu_img.height * ratio)
            ws.add_image(wu_img, f"F{row}")
            max_img_height = max(max_img_height, wu_img.height)

        ws.row_dimensions[row].height = max(25, int(max_img_height * 0.75))
        row += 1

    last_result_row = max(2, row - 1)
    ws.conditional_formatting.add(
        f"B2:B{last_result_row}",
        FormulaRule(formula=['B2="OK"'], fill=PatternFill("solid", fgColor="D1FAE5")),
    )
    ws.conditional_formatting.add(
        f"B2:B{last_result_row}",
        FormulaRule(formula=['B2="NG"'], fill=PatternFill("solid", fgColor="FEE2E2")),
    )
    ws.conditional_formatting.add(
        f"C2:C{last_result_row}",
        CellIsRule(operator="lessThan", formula=[str(BU_SPEC_MIN)], fill=PatternFill("solid", fgColor="FEF3C7")),
    )
    ws.conditional_formatting.add(
        f"E2:E{last_result_row}",
        CellIsRule(operator="lessThan", formula=[str(WU_SPEC_MIN)], fill=PatternFill("solid", fgColor="FEF3C7")),
    )

    detail_ws = wb.create_sheet("경로_중복정리")
    detail_ws.append(
        [
            "RecordType",
            "LotID",
            "Kind",
            "Status",
            "DuplicationFlag",
            "PathA",
            "PathB",
            "Etc",
        ]
    )

    for idx, rec in enumerate(records, start=1):
        ensure_not_cancelled(cancel_check)
        bbox_text = "" if rec["bbox"] is None else str(rec["bbox"])
        dst_text = "" if rec["dst"] is None else str(rec["dst"])
        detail_ws.append(
            [
                "CROP",
                rec["lot_id"],
                rec["kind"],
                rec["status"],
                rec.get("renamed_on_save", "FALSE"),
                str(rec["src"]),
                dst_text,
                bbox_text,
            ]
        )
        if idx == 1 or idx % 10 == 0 or idx == total:
            print_progress("  엑셀 진행", idx, total, done=(idx == total))

    if merge_rows:
        for row in merge_rows:
            ensure_not_cancelled(cancel_check)
            detail_ws.append(
                [
                    "MERGE",
                    row["lot_id"],
                    "",
                    "",
                    "TRUE" if row.get("selected_latest_final") == "FALSE" else "FALSE",
                    row.get("folder_path", ""),
                    "",
                    f"created={row.get('created_time','')}, modified={row.get('modified_time','')}",
                ]
            )

    if measurement_rows:
        for row in measurement_rows:
            ensure_not_cancelled(cancel_check)
            detail_ws.append(
                [
                    "MEASURE",
                    row["lot_id"],
                    "",
                    row["judge"],
                    "FALSE" if row.get("selected_latest_final") == "TRUE" else "TRUE",
                    row["source_file"],
                    "",
                    f"time={row['time_raw']}, BU={row['black_uniformity']}, WU={row['white_uniformity']}",
                ]
            )

    ws.column_dimensions["A"].width = 26
    ws.column_dimensions["B"].width = 12
    ws.column_dimensions["C"].width = 14
    ws.column_dimensions["D"].width = 36
    ws.column_dimensions["E"].width = 14
    ws.column_dimensions["F"].width = 36
    detail_ws.column_dimensions["A"].width = 12
    detail_ws.column_dimensions["B"].width = 26
    detail_ws.column_dimensions["C"].width = 10
    detail_ws.column_dimensions["D"].width = 24
    detail_ws.column_dimensions["E"].width = 16
    detail_ws.column_dimensions["F"].width = 60
    detail_ws.column_dimensions["G"].width = 60
    detail_ws.column_dimensions["H"].width = 44
    add_visualization_sheet(wb, latest_measurements or {})
    print("\n[7/7] 엑셀 저장")
    wb.save(excel_path)
    print("  엑셀 저장 완료")


def run_pipeline(integrated_root: Path, data_root: Path, threshold: int, padding: int, cancel_check=None) -> dict:
    # GUI/CLI 공용 실행 함수
    merged_root = integrated_root.parent / f"{integrated_root.name}_LotID_latest_v1"
    cropped_root = integrated_root.parent / f"{integrated_root.name}_LotID_latest_v1_cropped_v1"
    excel_path = cropped_root / "crop_report.xlsx"

    print(f"\n📌 원본 통합 폴더: {integrated_root}")
    print(f"📌 측정 데이터 폴더: {data_root}")
    print(f"📌 정리 폴더(merge): {merged_root}")
    print(f"📌 크롭 폴더: {cropped_root}")
    print(f"📌 엑셀 리포트: {excel_path}")
    print(f"📌 임계값: {threshold}, 패딩: {padding}")

    ensure_not_cancelled(cancel_check)
    latest_by_lotid, merge_rows = collect_latest_lotid_folders(integrated_root, cancel_check=cancel_check)
    if not latest_by_lotid:
        print("⚠️ LotID 폴더를 찾지 못했어. 폴더 구조를 확인해줘.")
        return

    copy_latest_folders(latest_by_lotid, merged_root, cancel_check=cancel_check)
    merge_report_path = write_merge_report(merge_rows, merged_root)

    latest_measurements, measurement_rows = collect_latest_measurements(data_root, cancel_check=cancel_check)
    crop_records = crop_images(merged_root, cropped_root, threshold, padding, cancel_check=cancel_check)
    write_excel(
        crop_records,
        excel_path,
        merge_rows=merge_rows,
        latest_measurements=latest_measurements,
        measurement_rows=measurement_rows,
        cancel_check=cancel_check,
    )

    duplicate_count = len(merge_rows) - len(latest_by_lotid)
    ok_count = sum(1 for r in crop_records if r["status"] == "OK")
    nodetect_count = sum(1 for r in crop_records if r["status"] == "NO_OBJECT_DETECTED")
    error_count = sum(1 for r in crop_records if r["status"].startswith("ERROR"))

    print("\n--- 최종 결과 ---")
    print(f"Merge 스캔 LotID 폴더 수: {len(merge_rows)}")
    print(f"Merge 최종 LotID 수: {len(latest_by_lotid)}")
    print(f"Merge 중복 제외 수: {duplicate_count}")
    print(f"Crop 전체 이미지 수: {len(crop_records)}")
    print(f"측정 최신 LotID 수: {len(latest_measurements)}")
    print(f"Crop 성공: {ok_count}")
    print(f"Crop 객체 미검출(원본 유지): {nodetect_count}")
    print(f"Crop 오류: {error_count}")
    print(f"Merge 리포트: {merge_report_path}")
    print(f"Crop 엑셀: {excel_path}")
    print("완료!")

    return {
        "merged_root": merged_root,
        "cropped_root": cropped_root,
        "excel_path": excel_path,
        "merge_report_path": merge_report_path,
        "merge_rows": len(merge_rows),
        "latest_lotids": len(latest_by_lotid),
        "latest_measurements": len(latest_measurements),
        "crop_records": len(crop_records),
        "crop_ok": ok_count,
        "crop_nodetect": nodetect_count,
        "crop_error": error_count,
    }


def main():
    # 원클릭 실행 순서:
    # 1) 사용자 입력 수집
    # 2) LotID 최신 폴더 취합(merge)
    # 3) 측정 데이터 최신값 집계
    # 4) merge 결과를 자동 크롭
    # 5) 엑셀 리포트 생성
    print("\n--- BU Organize One Click v1 ---")
    integrated_root = ask_path("1) 이미지 통합 폴더 경로: ")
    if not integrated_root.exists() or not integrated_root.is_dir():
        print(f"🚨 폴더를 찾을 수 없어: {integrated_root}")
        return

    data_root = ask_path("2) 측정 데이터 상위 폴더 경로: ")
    if not data_root.exists() or not data_root.is_dir():
        print(f"🚨 측정 데이터 폴더를 찾을 수 없어: {data_root}")
        return

    threshold = ask_int("3) 비검정 판정 임계값(0~255)", 12)
    padding = ask_int("4) 크롭 패딩(px)", 8)
    run_pipeline(integrated_root, data_root, threshold, padding)


if __name__ == "__main__":
    main()
