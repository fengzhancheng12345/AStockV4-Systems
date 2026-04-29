# C系统 - 服务器自动化因子训练

## 部署说明

### 1. 从GitHub拉取最新代码
```bash
cd /opt
git clone https://github.com/fengzhancheng12345/AStockV4-Systems.git
cd AStockV4-Systems
```

### 2. 安装依赖
```bash
pip3 install requests schedule
```

### 3. 运行自动化训练
```bash
cd scripts/v4_system/server_automated
python3 automated_factor_training.py &
```

### 4. 设置每日自动运行 (PM2)
```bash
pm2 start automated_factor_training.py --name astock-c
pm2 save
pm2 startup
```

### 5. 查看日志
```bash
pm2 logs astock-c
```

## 功能说明
- 全市场4000+股票每日采集
- 自动因子挖掘和IC分析
- 因子库动态更新
- 每日18:00自动运行
