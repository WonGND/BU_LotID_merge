# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path
import sysconfig
from PyInstaller.utils.hooks import collect_data_files

project_dir = Path.cwd()
python_base = Path(sysconfig.get_config_var("base"))
tcl_root = python_base / "tcl"
dll_root = python_base / "DLLs"

binaries = []
for dll_name in ("tcl86t.dll", "tk86t.dll"):
    dll_path = dll_root / dll_name
    if dll_path.exists():
        binaries.append((str(dll_path), "."))

datas = []
datas.extend(collect_data_files("tkinter"))

for source, target in (
    (project_dir / "tkinter", "tkinter"),
    (tcl_root / "tcl8.6", "_tcl_data"),
    (tcl_root / "tk8.6", "_tk_data"),
):
    if source.exists():
        datas.append((str(source), target))

a = Analysis(
    ['BU_organize_gui_v033.py'],
    pathex=[str(project_dir)],
    binaries=binaries,
    datas=datas,
    hiddenimports=['tkinter', '_tkinter', 'cv2'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='TOVIS_BU_DATA_정리_v0.3.3',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['tovis_bu_data.ico'],
)
