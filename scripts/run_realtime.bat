@echo off
chcp 65001 >nul
echo ========================================
echo   B站实时评论回复器
echo ========================================
echo.
echo 提示：请确保已配置 data/config/realtime.ini
echo.
python -m adapters.bilibili.bilibili_realtime %*
pause
