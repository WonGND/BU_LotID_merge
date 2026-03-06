import csv
import shutil
from datetime import datetime
from pathlib import Path

ALLOWED_EXTENSIONS = (".png", ".jpg", ".jpeg", ".bmp", ".tif", ".tiff", ".webp")


def folder_time_key(path: Path) -> tuple[float, float]:
    stat = path.stat()
    # Windows에서는 ctime이 생성 시각으로 동작한다.
    return (stat.st_ctime, stat.st_mtime)


def is_lotid_folder(path: Path) -> bool:
    if not path.is_dir():
        return False
    image_count = 0
    for child in path.iterdir():
        if child.is_file() and child.suffix.lower() in ALLOWED_EXTENSIONS:
            image_count += 1
    return image_count >= 1


def format_ts(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def collect_latest_lotid_folders(integrated_root: Path) -> tuple[dict[str, Path], list[dict]]:
    latest_by_lotid: dict[str, Path] = {}
    rows: list[dict] = []

    for p in integrated_root.rglob("*"):
        if not is_lotid_folder(p):
            continue

        lot_id = p.name
        current = latest_by_lotid.get(lot_id)
        is_latest = False

        if current is None:
            latest_by_lotid[lot_id] = p
            is_latest = True
        else:
            if folder_time_key(p) > folder_time_key(current):
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

    # 스캔 순서 때문에 selected_latest_at_scan_time은 최종 상태와 다를 수 있어 보정한다.
    selected_paths = {str(v) for v in latest_by_lotid.values()}
    for row in rows:
        row["selected_latest_final"] = "TRUE" if row["folder_path"] in selected_paths else "FALSE"

    return latest_by_lotid, rows


def copy_latest_folders(latest_by_lotid: dict[str, Path], output_root: Path) -> None:
    if output_root.exists():
        shutil.rmtree(output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    for lot_id, src in sorted(latest_by_lotid.items()):
        dst = output_root / lot_id
        shutil.copytree(src, dst)


def write_report(rows: list[dict], output_root: Path) -> Path:
    report_path = output_root / "merge_report.csv"
    fieldnames = [
        "lot_id",
        "folder_path",
        "created_time",
        "modified_time",
        "selected_latest_at_scan_time",
        "selected_latest_final",
    ]
    with report_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return report_path


def main() -> None:
    print("\n--- LotID 최신 폴더 취합 v1 ---")
    raw_input_dir = input("👉 통합폴더 경로를 입력하세요: ").strip().replace('"', "")
    integrated_root = Path(raw_input_dir)

    if not integrated_root.exists() or not integrated_root.is_dir():
        print(f"🚨 폴더를 찾을 수 없어: {integrated_root}")
        return

    output_root = integrated_root.parent / f"{integrated_root.name}_LotID_latest_v1"
    print(f"\n📌 입력: {integrated_root}")
    print(f"📌 출력: {output_root}")

    latest_by_lotid, rows = collect_latest_lotid_folders(integrated_root)
    if not latest_by_lotid:
        print("⚠️ LotID 폴더를 찾지 못했어. 폴더 구조를 확인해줘.")
        return

    copy_latest_folders(latest_by_lotid, output_root)
    report_path = write_report(rows, output_root)

    duplicate_count = len(rows) - len(latest_by_lotid)
    print("\n--- 결과 ---")
    print(f"✅ 스캔한 LotID 폴더 수: {len(rows)}")
    print(f"✅ 최종 취합 LotID 수: {len(latest_by_lotid)}")
    print(f"ℹ️ 중복으로 제외된 폴더 수: {duplicate_count}")
    print(f"📝 리포트: {report_path}")
    print("완료!")


if __name__ == "__main__":
    main()
