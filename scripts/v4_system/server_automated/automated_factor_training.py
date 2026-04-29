"""
Automated Factor Training System (C系统 - 服务器)
自动化因子训练系统

功能：
1. 每日自动运行
2. 全市场数据采集
3. 因子挖掘与测试
4. 因子库动态更新
5. 因子衰减监控
6. 自动生成报告
7. PM2进程管理

运行方式：
    pm2 start automated_factor_training.py --name astock_automated --interpreter python3
    pm2 logs astock_automated
    pm2 stop astock_automated
"""

import json
import time
import random
import urllib.request
import urllib.error
import os
import sys
import logging
import traceback
from datetime import datetime, timedelta
from collections import defaultdict
import math

# ==================== 配置 ====================

CONFIG = {
    'name': 'AStock Automated Factor Training',
    'version': 'C1.0',
    'data_dir': '/root/astock_automated/data',
    'factor_dir': '/root/astock_automated/factor_library',
    'log_dir': '/root/astock_automated/logs',
    'report_dir': '/root/astock_automated/reports',
    'max_stocks': 1000,  # 全市场
    'min_stocks': 500,
    'data_days': 500,
    'run_interval_hours': 6,  # 每6小时运行一次
    'ic_threshold': 0.02,
    'sharpe_threshold': 0.3,
    'decay_threshold': 0.5,  # 因子衰减阈值
    'github_repo': 'fengzhengcheng12345/AStockV4',
    'github_token': os.environ.get('GITHUB_TOKEN', ''),  # 从环境变量读取
}

# 确保目录存在
for d in [CONFIG['data_dir'], CONFIG['factor_dir'], CONFIG['log_dir'], CONFIG['report_dir']]:
    os.makedirs(d, exist_ok=True)

# ==================== 日志配置 ====================

def setup_logging():
    """配置日志"""
    log_file = os.path.join(CONFIG['log_dir'], f'astock_{datetime.now().strftime("%Y%m%d")}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger('astock')

logger = setup_logging()

# ==================== 数据采集 ====================

def get_kline_tencent(code='sh600000', scale=240, datalen=5000):
    """腾讯财经K线接口"""
    if code.startswith('sh') or code.startswith('sz'):
        sym = code
    else:
        sym = f'sh{code}' if code.startswith(('6', '9')) else f'sz{code}'
    
    url = f'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?_var=kline_dayhfq&param={sym},day,,,,{datalen},qfq'
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'https://finance.qq.com'})
        with urllib.request.urlopen(req, timeout=15) as r:
            text = r.read().decode('utf-8', errors='ignore')
            text = text.replace('kline_dayhfq=', '', 1).strip()
            data = json.loads(text)
            if not data or 'data' not in data or sym not in data['data']:
                return []
            qfqdata = data['data'][sym]['qfqday']
            if not qfqdata:
                return []
            result = []
            for d in qfqdata[-datalen:]:
                if len(d) >= 6:
                    result.append({
                        'day': d[0], 'open': float(d[1]), 'high': float(d[2]),
                        'low': float(d[3]), 'close': float(d[4]), 'volume': float(d[5])
                    })
            return result
    except Exception as e:
        logger.debug(f"获取K线失败 {code}: {e}")
        return []

def get_stock_list_a_share():
    """获取全市场A股列表"""
    url = 'https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=5000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048&fields=f12,f14,f3,f6,f7,f8,f104,f105,f106'
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'https://quote.eastmoney.com'})
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read().decode('utf-8', errors='ignore'))
            stocks = []
            if data and 'data' in data and 'diff' in data['data']:
                for item in data['data']['diff']:
                    code = item.get('f12', '')
                    name = item.get('f14', '')
                    if code and name and not name.startswith('*') and not name.startswith('ST'):
                        stocks.append({'code': code, 'name': name})
            return stocks
    except Exception as e:
        logger.error(f"获取股票列表失败: {e}")
        return []

# ==================== 技术指标 ====================

def calc_rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    return 100 - (100 / (1 + avg_gain / avg_loss))

def calc_kdj(highs, lows, closes, period=9):
    if len(closes) < period:
        return 50, 50, 50
    rsv = []
    for i in range(period - 1, len(closes)):
        high = max(highs[i - period + 1:i + 1])
        low = min(lows[i - period + 1:i + 1])
        if high == low:
            rsv.append(50)
        else:
            rsv.append((closes[i] - low) / (high - low) * 100)
    k = 50.0
    d = 50.0
    for r in rsv:
        k = (2 * k + r) / 3
        d = (2 * d + k) / 3
    j = 3 * k - 2 * d
    return k, d, j

def calc_cci(highs, lows, closes, period=14):
    if len(closes) < period:
        return 0
    typical = [(highs[i] + lows[i] + closes[i]) / 3 for i in range(len(closes))]
    sma = sum(typical[-period:]) / period
    mean_dev = sum(abs(typical[i] - sma) for i in range(len(typical) - period, len(typical))) / period
    if mean_dev == 0:
        return 0
    return (typical[-1] - sma) / (0.015 * mean_dev)

def calc_williams_r(highs, lows, closes):
    if len(closes) < 14:
        return -50
    period = 14
    high = max(highs[-period:])
    low = min(lows[-period:])
    if high == low:
        return -50
    return (high - closes[-1]) / (high - low) * -100

def calc_bollinger_position(closes, period=20):
    if len(closes) < period:
        return 0.5
    ma = sum(closes[-period:]) / period
    variance = sum((c - ma) ** 2 for c in closes[-period:]) / period
    std = math.sqrt(variance)
    upper = ma + 2 * std
    lower = ma - 2 * std
    if upper == lower:
        return 0.5
    return (closes[-1] - lower) / (upper - lower)

# ==================== 因子生成 ====================

def generate_factors_for_stock(klines):
    """为单只股票生成所有候选因子"""
    if len(klines) < 70:
        return []
    
    samples = []
    
    for i in range(40, len(klines) - 5):
        try:
            closes = [klines[j]['close'] for j in range(max(0, i - 59), i + 1)]
            highs = [klines[j]['high'] for j in range(max(0, i - 59), i + 1)]
            lows = [klines[j]['low'] for j in range(max(0, i - 59), i + 1)]
            vols = [float(klines[j]['volume']) for j in range(max(0, i - 59), i + 1)]
            
            if len(closes) < 60:
                continue
            
            sample = {'day': klines[i].get('day', '')}
            
            # 价格因子
            for n in [1, 5, 10, 20, 30, 60]:
                if i >= n and klines[i-n]['close'] > 0:
                    pct = (closes[-1] - klines[i-n]['close']) / klines[i-n]['close'] * 100
                else:
                    pct = 0
                sample[f'pct_{n}d'] = pct
            
            # RSI
            for period in [6, 9, 14, 26]:
                sample[f'rsi_{period}'] = calc_rsi(closes, period)
            
            # KDJ
            k, d, j = calc_kdj(highs, lows, closes)
            sample['kdj_k'] = k
            sample['kdj_d'] = d
            sample['kdj_j'] = j
            
            # CCI
            sample['cci_14'] = calc_cci(highs, lows, closes, 14)
            sample['cci_20'] = calc_cci(highs, lows, closes, 20)
            
            # 威廉指标
            sample['williams_r'] = calc_williams_r(highs, lows, closes)
            
            # 布林带
            sample['boll_pos_20'] = calc_bollinger_position(closes, 20)
            sample['boll_pos_30'] = calc_bollinger_position(closes, 30)
            
            # 成交量
            for period in [5, 10, 20]:
                if len(vols) >= period + 1:
                    avg_vol = sum(vols[-period-1:-1]) / period
                    sample[f'vr_{period}'] = vols[-1] / avg_vol if avg_vol > 0 else 1
                else:
                    sample[f'vr_{period}'] = 1
            
            # 均线
            for period in [5, 10, 20, 30, 60]:
                if len(closes) >= period:
                    ma = sum(closes[-period:]) / period
                    sample[f'price_ma{period}'] = closes[-1] / ma if ma > 0 else 1
                else:
                    sample[f'price_ma{period}'] = 1
            
            # 振幅
            if closes[-1] > 0:
                sample['amplitude'] = (highs[-1] - lows[-1]) / closes[-1] * 100
            else:
                sample['amplitude'] = 0
            
            # T+1目标
            if i + 1 < len(klines):
                next_open = klines[i + 1]['open']
                if next_open > 0:
                    sample['t1_pct'] = (klines[i + 1]['close'] - next_open) / next_open * 100
                    sample['t1_up'] = 1 if sample['t1_pct'] > 0 else 0
                else:
                    sample['t1_pct'] = 0
                    sample['t1_up'] = 0
            else:
                sample['t1_pct'] = 0
                sample['t1_up'] = 0
            
            samples.append(sample)
        except:
            continue
    
    return samples

# ==================== 因子测试 ====================

def calculate_ic(factor_values, target_returns):
    """计算信息系数"""
    valid_pairs = [(f, t) for f, t in zip(factor_values, target_returns) 
                   if f is not None and t is not None and not math.isnan(f) and not math.isnan(t)]
    if len(valid_pairs) < 30:
        return 0
    
    factor_vals = [v[0] for v in valid_pairs]
    ret_vals = [v[1] for v in valid_pairs]
    
    mean_factor = sum(factor_vals) / len(factor_vals)
    mean_ret = sum(ret_vals) / len(ret_vals)
    
    cov = sum((f - mean_factor) * (r - mean_ret) for f, r in valid_pairs) / len(valid_pairs)
    std_factor = (sum((f - mean_factor) ** 2 for f in factor_vals) / len(factor_vals)) ** 0.5
    std_ret = (sum((r - mean_ret) ** 2 for r in ret_vals) / len(ret_vals)) ** 0.5
    
    if std_factor == 0 or std_ret == 0:
        return 0
    
    return cov / (std_factor * std_ret)

def quantile_test(factor_values, target_returns, quantiles=5):
    """分层回测"""
    valid_pairs = [(f, t) for f, t in zip(factor_values, target_returns) 
                   if f is not None and t is not None and not math.isnan(f) and not math.isnan(t)]
    if len(valid_pairs) < quantiles * 10:
        return None
    
    sorted_pairs = sorted(valid_pairs, key=lambda x: x[0])
    n = len(sorted_pairs)
    q_size = n // quantiles
    
    returns_by_quantile = []
    for i in range(quantiles):
        start = i * q_size
        end = start + q_size if i < quantiles - 1 else n
        q_returns = [p[1] for p in sorted_pairs[start:end]]
        avg_ret = sum(q_returns) / len(q_returns) if q_returns else 0
        returns_by_quantile.append(avg_ret)
    
    spread = returns_by_quantile[-1] - returns_by_quantile[0]
    return {'spread': spread, 'returns': returns_by_quantile}

def long_short_test(factor_values, target_returns):
    """多空组合"""
    valid_pairs = [(f, t) for f, t in zip(factor_values, target_returns) 
                   if f is not None and t is not None and not math.isnan(f) and not math.isnan(t)]
    if len(valid_pairs) < 30:
        return {'spread': 0, 'sharpe': 0}
    
    sorted_pairs = sorted(valid_pairs, key=lambda x: x[0])
    n = len(sorted_pairs)
    top_n = max(1, n // 5)
    
    long_returns = [p[1] for p in sorted_pairs[-top_n:]]
    short_returns = [p[1] for p in sorted_pairs[:top_n]]
    
    spread = (sum(long_returns) / len(long_returns)) - (sum(short_returns) / len(short_returns))
    sharpe = spread / (sum(long_returns + short_returns) / len(long_returns + short_returns) + 0.01)
    
    return {'spread': spread, 'sharpe': sharpe}

# ==================== 因子库管理 ====================

class FactorLibrary:
    """因子库管理"""
    
    def __init__(self, library_path):
        self.library_path = library_path
        self.factors = []
        self.load()
    
    def load(self):
        """加载因子库"""
        if os.path.exists(self.library_path):
            try:
                with open(self.library_path, 'r') as f:
                    data = json.load(f)
                    self.factors = data.get('factors', [])
                logger.info(f"加载因子库: {len(self.factors)} 个因子")
            except Exception as e:
                logger.error(f"加载因子库失败: {e}")
                self.factors = []
        else:
            logger.info("因子库为空，将创建新库")
            self.factors = []
    
    def save(self):
        """保存因子库"""
        data = {
            'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'factors': self.factors
        }
        try:
            with open(self.library_path, 'w') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"因子库已保存: {self.library_path}")
        except Exception as e:
            logger.error(f"保存因子库失败: {e}")
    
    def add_factor(self, factor_data):
        """添加因子"""
        # 检查是否已存在
        existing = next((f for f in self.factors if f['name'] == factor_data['name']), None)
        if existing:
            # 更新历史
            if 'history' not in existing:
                existing['history'] = []
            existing['history'].append({
                'date': datetime.now().strftime('%Y-%m-%d'),
                'ic': existing.get('ic', 0),
                'sharpe': existing.get('sharpe', 0)
            })
            existing.update(factor_data)
            existing['last_update'] = datetime.now().strftime('%Y-%m-%d')
        else:
            factor_data['added_date'] = datetime.now().strftime('%Y-%m-%d')
            factor_data['last_update'] = datetime.now().strftime('%Y-%m-%d')
            factor_data['history'] = []
            self.factors.append(factor_data)
    
    def remove_factor(self, factor_name):
        """移除因子"""
        self.factors = [f for f in self.factors if f['name'] != factor_name]
    
    def check_decay(self, factor_data, current_ic, threshold=0.5):
        """检查因子衰减"""
        if 'history' not in factor_data or len(factor_data['history']) < 3:
            return False
        
        historical_ics = [h['ic'] for h in factor_data['history'][-5:]]
        avg_historical = sum(historical_ics) / len(historical_ics)
        
        if avg_historical == 0:
            return False
        
        decay_ratio = abs(current_ic) / abs(avg_historical)
        
        return decay_ratio < threshold
    
    def update_weights(self):
        """更新因子权重"""
        if not self.factors:
            return
        
        # 基于IC和夏普计算权重
        for f in self.factors:
            ic = abs(f.get('ic', 0))
            sharpe = abs(f.get('sharpe', 0))
            f['weight'] = (ic * 10 + sharpe) / 2
        
        # 归一化
        total = sum(f['weight'] for f in self.factors)
        if total > 0:
            for f in self.factors:
                f['weight'] /= total


# ==================== 主系统 ====================

class AutomatedFactorTrainingSystem:
    """自动化因子训练系统"""
    
    def __init__(self):
        self.config = CONFIG
        self.running = True
        self.library = FactorLibrary(
            os.path.join(self.config['factor_dir'], 'factor_library.json')
        )
        self.daily_results = []
        
        logger.info("=" * 60)
        logger.info("AStock Automated Factor Training System - C系统")
        logger.info("=" * 60)
    
    def collect_data(self, stock_list):
        """采集股票数据"""
        all_samples = []
        collected = 0
        failed = 0
        
        max_stocks = min(self.config['max_stocks'], len(stock_list))
        
        logger.info(f"开始采集 {max_stocks} 只股票数据...")
        
        for i, stock in enumerate(stock_list[:max_stocks]):
            code = stock['code']
            
            klines = get_kline_tencent(code, datalen=self.config['data_days'])
            
            if klines and len(klines) > 60:
                samples = generate_factors_for_stock(klines)
                if samples:
                    for s in samples:
                        s['stock_code'] = code
                        s['stock_name'] = stock['name']
                    all_samples.extend(samples)
                    collected += 1
            else:
                failed += 1
            
            if (i + 1) % 100 == 0:
                logger.info(f"  进度: {i+1}/{max_stocks}  成功: {collected}  失败: {failed}  样本: {len(all_samples)}")
            
            time.sleep(0.05)
        
        logger.info(f"采集完成: {collected} 只股票, {len(all_samples)} 样本")
        return all_samples
    
    def test_factors(self, samples):
        """测试因子"""
        exclude_fields = {'day', 'stock_code', 'stock_name', 't1_pct', 't1_up'}
        factor_names = [k for k in samples[0].keys() if k not in exclude_fields]
        
        target_returns = [s['t1_pct'] for s in samples]
        
        logger.info(f"测试 {len(factor_names)} 个因子...")
        
        results = []
        
        for fname in factor_names:
            try:
                factor_values = [s.get(fname) for s in samples]
                
                ic = calculate_ic(factor_values, target_returns)
                quantile = quantile_test(factor_values, target_returns)
                ls = long_short_test(factor_values, target_returns)
                
                result = {
                    'name': fname,
                    'ic': ic,
                    'ic_abs': abs(ic),
                    'spread': ls['spread'] if ls else 0,
                    'sharpe': ls['sharpe'] if ls else 0,
                    'valid': len([v for v in factor_values if v is not None and not math.isnan(v)])
                }
                
                results.append(result)
                
            except Exception as e:
                logger.debug(f"因子测试失败 {fname}: {e}")
        
        results.sort(key=lambda x: -x['ic_abs'])
        return results
    
    def update_factor_library(self, new_factors):
        """更新因子库"""
        logger.info("更新因子库...")
        
        for r in new_factors:
            if r['ic_abs'] >= self.config['ic_threshold'] and r['valid'] >= 100:
                self.library.add_factor(r)
        
        # 检查衰减
        decayed_factors = []
        for f in self.library.factors[:]:
            if self.library.check_decay(f, f.get('ic', 0), self.config['decay_threshold']):
                decayed_factors.append(f['name'])
                self.library.remove_factor(f['name'])
        
        if decayed_factors:
            logger.warning(f"移除衰减因子: {decayed_factors}")
        
        # 更新权重
        self.library.update_weights()
        
        # 保存
        self.library.save()
        
        return len(new_factors), len(decayed_factors)
    
    def generate_report(self, samples_count, new_factors_count, decayed_count):
        """生成报告"""
        report = {
            'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'samples_collected': samples_count,
            'new_factors_added': new_factors_count,
            'factors_decayed': decayed_count,
            'total_factors': len(self.library.factors),
            'top_factors': sorted(self.library.factors, key=lambda x: -x.get('ic_abs', 0))[:10]
        }
        
        report_file = os.path.join(
            self.config['report_dir'], 
            f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        
        with open(report_file, 'w') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        logger.info(f"报告已生成: {report_file}")
        
        # 也生成日报
        daily_report = os.path.join(self.config['report_dir'], 'daily_report.json')
        with open(daily_report, 'w') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        return report
    
    def run_daily_cycle(self):
        """执行每日循环"""
        logger.info("\n" + "=" * 60)
        logger.info(f"开始每日训练 cycle - {datetime.now()}")
        logger.info("=" * 60)
        
        try:
            # Step 1: 获取股票列表
            logger.info("[Step 1] 获取股票列表...")
            stock_list = get_stock_list_a_share()
            logger.info(f"获取到 {len(stock_list)} 只股票")
            
            if not stock_list:
                logger.error("获取股票列表失败!")
                return False
            
            # Step 2: 采集数据
            samples = self.collect_data(stock_list)
            
            if not samples:
                logger.error("采集数据失败!")
                return False
            
            # 保存原始数据
            data_file = os.path.join(
                self.config['data_dir'],
                f"samples_{datetime.now().strftime('%Y%m%d')}.json"
            )
            with open(data_file, 'w') as f:
                json.dump(samples[:100000], f, ensure_ascii=False)
            logger.info(f"原始数据已保存: {data_file}")
            
            # Step 3: 测试因子
            results = self.test_factors(samples)
            
            # 保存测试结果
            results_file = os.path.join(
                self.config['factor_dir'],
                f"test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            )
            with open(results_file, 'w') as f:
                json.dump(results[:100], f, ensure_ascii=False, indent=2)
            
            # Step 4: 更新因子库
            new_count, decayed_count = self.update_factor_library(results)
            
            # Step 5: 生成报告
            report = self.generate_report(len(samples), new_count, decayed_count)
            
            logger.info("\n" + "=" * 60)
            logger.info("【训练完成】")
            logger.info(f"  样本数: {report['samples_collected']}")
            logger.info(f"  新增因子: {report['new_factors_added']}")
            logger.info(f"  衰减移除: {report['factors_decayed']}")
            logger.info(f"  因子库总数: {report['total_factors']}")
            logger.info("=" * 60)
            
            # 打印TOP因子
            if report['top_factors']:
                logger.info("\n【TOP 10 因子】")
                for i, f in enumerate(report['top_factors'][:10], 1):
                    logger.info(f"  {i}. {f['name']}: IC={f.get('ic', 0):.4f}, Sharpe={f.get('sharpe', 0):.4f}")
            
            return True
            
        except Exception as e:
            logger.error(f"训练循环出错: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def run(self):
        """运行系统"""
        logger.info("系统启动，等待任务...")
        
        while self.running:
            try:
                # 运行一次训练
                success = self.run_daily_cycle()
                
                if success:
                    logger.info(f"本次训练成功，等待 {self.config['run_interval_hours']} 小时后再次运行...")
                else:
                    logger.warning("本次训练失败，30分钟后重试...")
                    time.sleep(1800)  # 30分钟
                
                # 简单循环：运行一次后退出，让PM2管理重启
                # 如果需要持续运行，取消下面的break
                # time.sleep(self.config['run_interval_hours'] * 3600)
                break
                
            except KeyboardInterrupt:
                logger.info("收到停止信号，系统关闭...")
                self.running = False
                break
            except Exception as e:
                logger.error(f"系统错误: {e}")
                logger.error(traceback.format_exc())
                time.sleep(300)  # 5分钟后重试
        
        logger.info("系统已停止")


# ==================== 主程序 ====================

if __name__ == '__main__':
    system = AutomatedFactorTrainingSystem()
    system.run()