from BU_organize_gui_shared import GuiConfig, launch_app
from BU_organize_one_click_v02 import PipelineCancelled, run_pipeline

CONFIG = GuiConfig(
    app_name="TOVIS_BU_DATA_정리_v0.2",
    icon_filename="tovis_bu_data.ico",
    settings_filename="tovis_bu_data_settings_v02.json",
    log_filename="TOVIS_BU_DATA_정리_v0.2_log.txt",
    splash_version_text="버전 v0.2 로딩 중...",
    splash_done_token="splash_done_v02.tmp",
)


def main() -> None:
    launch_app(CONFIG, run_pipeline, PipelineCancelled)


if __name__ == "__main__":
    main()
