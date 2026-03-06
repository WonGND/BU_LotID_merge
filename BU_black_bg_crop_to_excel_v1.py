import re
from pathlib import Path

from openpyxl import Workbook
from openpyxl.drawing.image import Image as XLImage
from PIL import Image

ALLOWED_EXTENSIONS = (".png", ".jpg", ".jpeg", ".bmp", ".tif", ".tiff", ".webp")
LOT_PATTERN = re.compile(r"^(?P<lotid>.+)_(?P<kind>BU|WU)_\d+$", re.IGNORECASE)


def ask_int(prompt: str, default: int) -> int:
    raw = input(f"{prompt} (기본값 {default}): ").strip().replace('"', "")
    if not raw:
        return default
    return int(raw)


def find_non_black_bbox(img: Image.Image, threshold: int = 12):
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
    left, top, right, bottom = box
    return (
        max(0, left - pad),
        max(0, top - pad),
        min(w, right + pad),
        min(h, bottom + pad),
    )


def parse_lot_kind(stem: str):
    m = LOT_PATTERN.match(stem)
    if not m:
        return stem, "UNKNOWN"
    return m.group("lotid"), m.group("kind").upper()


def crop_images(input_root: Path, output_root: Path, threshold: int, padding: int):
    if output_root.exists():
        for p in output_root.rglob("*"):
            if p.is_file():
                p.unlink()
        for d in sorted((x for x in output_root.rglob("*") if x.is_dir()), reverse=True):
            d.rmdir()
    output_root.mkdir(parents=True, exist_ok=True)

    records = []
    for src in input_root.rglob("*"):
        if not src.is_file() or src.suffix.lower() not in ALLOWED_EXTENSIONS:
            continue

        rel = src.relative_to(input_root)
        dst = output_root / rel
        dst.parent.mkdir(parents=True, exist_ok=True)

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
                }
            )
    return records


def write_excel(records, excel_path: Path, image_width_px: int = 240):
    wb = Workbook()
    ws = wb.active
    ws.title = "cropped_images"
    ws.append(["LotID", "Kind", "Status", "CropBBox", "SourcePath", "CroppedPath", "Preview"])

    row = 2
    for rec in records:
        bbox_text = "" if rec["bbox"] is None else str(rec["bbox"])
        dst_text = "" if rec["dst"] is None else str(rec["dst"])
        ws.cell(row=row, column=1, value=rec["lot_id"])
        ws.cell(row=row, column=2, value=rec["kind"])
        ws.cell(row=row, column=3, value=rec["status"])
        ws.cell(row=row, column=4, value=bbox_text)
        ws.cell(row=row, column=5, value=str(rec["src"]))
        ws.cell(row=row, column=6, value=dst_text)

        if rec["dst"] is not None and rec["dst"].exists():
            img = XLImage(str(rec["dst"]))
            if img.width > image_width_px:
                ratio = image_width_px / img.width
                img.width = int(img.width * ratio)
                img.height = int(img.height * ratio)
            ws.add_image(img, f"G{row}")
            ws.row_dimensions[row].height = max(80, int(img.height * 0.75))

        row += 1

    ws.column_dimensions["A"].width = 26
    ws.column_dimensions["B"].width = 10
    ws.column_dimensions["C"].width = 24
    ws.column_dimensions["D"].width = 25
    ws.column_dimensions["E"].width = 60
    ws.column_dimensions["F"].width = 60
    ws.column_dimensions["G"].width = 40
    wb.save(excel_path)


def main():
    print("\n--- 검은 배경 이미지 자동 크롭 + 엑셀 삽입 v1 ---")
    raw = input("👉 입력 폴더 경로: ").strip().replace('"', "")
    input_root = Path(raw)
    if not input_root.exists() or not input_root.is_dir():
        print(f"🚨 폴더가 없어: {input_root}")
        return

    threshold = ask_int("비검정 판정 임계값(0~255)", 12)
    padding = ask_int("크롭 패딩(px)", 8)

    output_root = input_root.parent / f"{input_root.name}_cropped_v1"
    excel_path = output_root / "crop_report.xlsx"

    print(f"\n📌 입력: {input_root}")
    print(f"📌 출력: {output_root}")
    print(f"📌 임계값: {threshold}, 패딩: {padding}")

    records = crop_images(input_root, output_root, threshold, padding)
    write_excel(records, excel_path)

    ok_count = sum(1 for r in records if r["status"] == "OK")
    nodetect_count = sum(1 for r in records if r["status"] == "NO_OBJECT_DETECTED")
    error_count = sum(1 for r in records if r["status"].startswith("ERROR"))

    print("\n--- 결과 ---")
    print(f"전체 이미지: {len(records)}")
    print(f"크롭 성공: {ok_count}")
    print(f"객체 미검출(원본 유지): {nodetect_count}")
    print(f"오류: {error_count}")
    print(f"엑셀: {excel_path}")
    print("완료!")


if __name__ == "__main__":
    main()
