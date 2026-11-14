#!/usr/bin/env bash
# نمونه‌ی اجرای روزانه — ساعت پایه و هسته‌ها را با سرور خودتان تنظیم کنید.
# چند دقیقه قبل از بازار اجرا شود (موتور خودش ۳ثانیه قبل از اولین شلیک اتصال‌ها را گرم می‌کند).
set -e
cd "$(dirname "$0")"
export TZ=Asia/Tehran
exec ./sarkh_engine run \
  --accounts accounts.json \
  --base 08:44:50 \
  --send-cpus 2,3,4,5 --conn-cpus 6,7 --read-cpus 1 \
  --rt --spin-us 2000 \
  --out engine_out
