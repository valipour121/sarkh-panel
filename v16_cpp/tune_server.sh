#!/usr/bin/env bash
# تنظیمات پیشنهادی Ubuntu 22.04/24.04 برای زمان‌بندی دقیق (با root اجرا شود؛ قبل از اجرا بخوانید)
set -e
apt-get update && apt-get install -y chrony g++ make libssl-dev zlib1g-dev
timedatectl set-timezone Asia/Tehran
# ساعت: چند سرور NTP (ترجیحاً NTP داخلیِ دیتاسنتر یا ایرانی) در /etc/chrony/chrony.conf بگذارید، سپس:
systemctl enable --now chrony
# پایدارسازی فرکانس CPU
for g in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do [ -f "$g" ] && echo performance > "$g" || true; done
echo "بررسی ساعت:  chronyc tracking   (System time باید کمتر از ~1ms باشد)"
echo "برای جداکردن هسته‌ها (اختیاری): در GRUB اضافه کنید  isolcpus=2-7 nohz_full=2-7  و ریبوت کنید"
