import csv
import shutil
from datetime import datetime
from pathlib import Path

# 처리 대상 이미지 확장자 (필요하면 여기서 추가/삭제)
ALLOWED_EXTENSIONS = (".png", ".jpg", ".jpeg", ".bmp", ".tif", ".tiff", ".webp")


def print_progress(label: str, current: int, total: int, done: bool = False) -> None:
    # 진행률 표시 공통 함수
    if total <= 0:
        return
    percent = (current / total) * 100
    end = "\n" if done else "\r"
    print(f"{label}: {current}/{total} ({percent:5.1f}%)", end=end, flush=True)


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


def folder_time_key(path: Path) -> tuple[float, float]:
    # 최신 폴더 비교 기준: 생성시각 우선, 동일하면 수정시각
    stat = path.stat()
    # Windows에서는 ctime이 생성 시각으로 동작한다.
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


def collect_latest_lotid_folders(integrated_root: Path) -> tuple[dict[str, Path], list[dict]]:
    # 전체 폴더를 훑어서 LotID별 최신 폴더 1개만 남긴다.
    latest_by_lotid: dict[str, Path] = {}
    rows: list[dict] = []

    dir_candidates = [p for p in integrated_root.rglob("*") if p.is_dir()]
    total_dirs = len(dir_candidates)
    print(f"\n[1/3] LotID 폴더 스캔 시작 (대상 폴더: {total_dirs}개)")

    for idx, p in enumerate(dir_candidates, start=1):
        if idx == 1 or idx % 50 == 0 or idx == total_dirs:
            print_progress("  스캔 진행", idx, total_dirs, done=(idx == total_dirs))

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

    selected_paths = {str(v) for v in latest_by_lotid.values()}
    for row in rows:
        row["selected_latest_final"] = "TRUE" if row["folder_path"] in selected_paths else "FALSE"

    return latest_by_lotid, rows


def copy_latest_folders(latest_by_lotid: dict[str, Path], output_root: Path) -> None:
    # 최종 선택된 LotID 폴더만 결과 폴더로 복사
    if output_root.exists():
        shutil.rmtree(output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    items = sorted(latest_by_lotid.items())
    total = len(items)
    print(f"\n[2/3] 최신 LotID 폴더 복사 시작 (대상: {total}개)")

    for idx, (lot_id, src) in enumerate(items, start=1):
        dst = unique_folder_path(output_root, lot_id)
        shutil.copytree(src, dst)
        if idx == 1 or idx % 20 == 0 or idx == total:
            print_progress("  복사 진행", idx, total, done=(idx == total))


def write_report(rows: list[dict], output_root: Path) -> Path:
    # 병합 판단 결과를 CSV로 저장
    report_path = output_root / "merge_report.csv"
    fieldnames = [
        "lot_id",
        "folder_path",
        "created_time",
        "modified_time",
        "selected_latest_at_scan_time",
        "selected_latest_final",
    ]
    print("\n[3/3] 리포트 저장")
    with report_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print("  리포트 저장 완료")
    return report_path


def main() -> None:
    # 실행 순서:
    # 1) 입력 폴더 확인
    # 2) 최신 LotID 선별
    # 3) 선별 폴더 복사 + 리포트 저장
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
