#!/bin/bash
# C系统服务器部署脚本
# 在阿里云服务器上执行

set -e

echo "=========================================="
echo "C系统 - 服务器部署脚本"
echo "=========================================="

# 1. 创建工作目录
echo "[1/6] 创建工作目录..."
mkdir -p /opt/astock
cd /opt/astock

# 2. 克隆GitHub仓库
echo "[2/6] 克隆GitHub仓库..."
git clone https://github.com/fengzhancheng12345/AStockV4-Systems.git .
git pull origin main

# 3. 安装依赖
echo "[3/6] 安装依赖..."
pip3 install requests schedule paramiko || pip install requests schedule paramiko

# 4. 创建必要目录
echo "[4/6] 创建目录..."
mkdir -p predictions/factor_library
mkdir -p predictions/daily_results
mkdir -p logs

# 5. 设置PM2
echo "[5/6] 设置PM2..."
if ! command -v pm2 &> /dev/null; then
    npm install -g pm2
fi

# 启动C系统
pm2 start scripts/v4_system/server_automated/automated_factor_training.py --name astock-c
pm2 save

# 6. 设置定时任务
echo "[6/6] 设置定时任务..."
(crontab -l 2>/dev/null | grep -v "automated_factor_training"; echo "0 18 * * * cd /opt/astock && python3 scripts/v4_system/server_automated/automated_factor_training.py >> logs/daily.log 2>&1") | crontab -

echo "=========================================="
echo "部署完成!"
echo "查看状态: pm2 status"
echo "查看日志: pm2 logs astock-c"
echo "=========================================="