# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['BU_organize_gui.py'],
    pathex=[],
    binaries=[],
    datas=[('C:\\Users\\TOVIS\\AppData\\Local\\Python\\pythoncore-3.14-64\\tcl\\tcl8.6', '_tcl_data'), ('C:\\Users\\TOVIS\\AppData\\Local\\Python\\pythoncore-3.14-64\\tcl\\tk8.6', '_tk_data'), ('tkinter', 'tkinter')],
    hiddenimports=['tkinter', '_tkinter'],
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
    name='TOVIS_BU_DATA_정리_v0.1',
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
