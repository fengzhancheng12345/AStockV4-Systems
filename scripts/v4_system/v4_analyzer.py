"""
V4系统 - 主分析脚本
V38.2 + 经验规则V3 + 市场环境自适应
"""

import json
import urllib.request
import time
from datetime import datetime
from typing import List, Dict, Optional

from technical_factors import (
    TechnicalFactors, V3ExperienceFactors, 
    MarketEnvironmentFactors, SectorSynergyFactors,
    AdaptiveFactorWeights, SignalFilter, CompositeScorer
)

class V4Analyzer:
    """V4综合分析器"""
    
    def __init__(self):
        self.tech = TechnicalFactors()
        self.v3 = V3ExperienceFactors()
        self.market_env = MarketEnvironmentFactors()
        self.sector = SectorSynergyFactors()
        self.weights = AdaptiveFactorWeights()
        self.filter = SignalFilter()
        self.scorer = CompositeScorer()
    
    def get_all_stocks(self) -> List[Dict]:
        """获取全市场股票列表"""
        stocks = []
        for node in ['cyb', 'kcb', 'hs_a']:
            for page in range(1, 10):
                url = f'https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeDataSimple?page={page}&num=50&sort=amount&asc=0&node={node}'
                try:
                    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'https://finance.sina.com.cn/'})
                    with urllib.request.urlopen(req, timeout=10) as r:
                        data = json.loads(r.read().decode('utf-8', errors='ignore'))
                    if not data:
                        break
                    for s in data:
                        code = s.get('code', '')
                        name = s.get('name', '')
                        pct = float(s.get('changepercent', 0))
                        price = float(s.get('trade', 0))
                        if any(kw in name for kw in ['ST', '*ST', '退', 'N']) or price <= 0 or price > 500:
                            continue
                        stocks.append({'code': code, 'name': name, 'pct': pct, 'price': price})
                except:
                    break
                time.sleep(0.1)
        return stocks
    
    def get_market_environment(self) -> Dict:
        """
        获取市场环境评分
        
        目前使用简化版（基于指数状态推断）
        完整版需要获取全市场涨跌家数
        """
        # 获取上证指数数据
        kl = self.tech.get_kl('000001', 30)
        if len(kl) < 20:
            return {'score': 0.5, 'condition': 'unknown', 'features': {}}
        
        closes = [k['close'] for k in kl]
        ma20 = sum(closes[-20:]) / 20
        ma5 = sum(closes[-5:]) / 5
        
        index_above_ma20 = closes[-1] > ma20
        ma5_above_ma20 = ma5 > ma20
        index_change = (closes[-1] - closes[-2]) / closes[-2] * 100 if len(closes) >= 2 else 0
        
        # 估算上涨家数比（基于指数表现估算）
        if index_change > 1:
            advance_ratio = 0.65
        elif index_change > 0.3:
            advance_ratio = 0.55
        elif index_change > 0:
            advance_ratio = 0.52
        elif index_change > -0.3:
            advance_ratio = 0.48
        elif index_change > -1:
            advance_ratio = 0.42
        else:
            advance_ratio = 0.35
        
        # 计算市场评分
        score = self.market_env.judge_market_condition(
            index_above_ma20=index_above_ma20,
            ma5_above_ma20=ma5_above_ma20,
            index_change=index_change,
            advance_ratio=advance_ratio,
            volume_ratio=1.0  # 简化
        )
        
        if score > 0.6:
            condition = '强势'
        elif score < 0.4:
            condition = '弱势'
        else:
            condition = '震荡'
        
        return {
            'score': score,
            'condition': condition,
            'features': {
                'index_above_ma20': index_above_ma20,
                'ma5_above_ma20': ma5_above_ma20,
                'index_change': index_change,
                'advance_ratio': advance_ratio
            }
        }
    
    def analyze_stocks(self, stocks: List[Dict], market_env: Dict) -> List[Dict]:
        """分析所有股票"""
        market_score = market_env['score']
        results = []
        
        for i, s in enumerate(stocks):
            code = s['code']
            
            # 获取技术因子
            factors = self.tech.calculate_all_factors(code)
            if not factors:
                continue
            
            # 计算V3评分
            v3_score, v3_reasons = self.v3.oversold_rebound_factor(
                factors['rsi_14'],
                factors['vr'],
                factors['change_pct']
            )
            
            # 调整权重
            adjusted_weights = self.weights.adjust_for_market_condition(market_score)
            
            # 计算综合评分
            # V3权重 + 动量 + 成交量 + 市场环境
            momentum_score = 0
            if factors['momentum_5'] > 0:
                momentum_score += 3
            if factors['ma5_above_ma20']:
                momentum_score += 2
            
            volume_score = 0
            if factors['vr'] > 1.2:
                volume_score += 3
            elif factors['vr'] > 1.0:
                volume_score += 1
            
            composite = (
                v3_score * adjusted_weights['v3_experience'] +
                momentum_score * adjusted_weights['momentum'] +
                volume_score * adjusted_weights['volume']
            )
            
            # 方向判断
            if composite > 5:
                direction = '推荐'
            elif composite < -5:
                direction = '规避'
            else:
                direction = '观察'
            
            # 置信度
            confidence = 0.5
            if abs(composite) > 10:
                confidence = 0.8
            elif abs(composite) > 5:
                confidence = 0.6
            
            results.append({
                'code': code,
                'name': s['name'],
                'price': s['price'],
                'pct': s['pct'],
                'rsi': factors['rsi_14'],
                'vr': factors['vr'],
                'change_5d': factors['change_5d'],
                'v3_score': v3_score,
                'v3_reasons': v3_reasons,
                'composite': composite,
                'direction': direction,
                'confidence': confidence,
                'market_condition': market_env['condition']
            })
            
            if (i + 1) % 100 == 0:
                print(f"  {i+1}/{len(stocks)} 完成")
            
            time.sleep(0.03)
        
        return results
    
    def filter_signals(self, signals: List[Dict], market_env: Dict) -> List[Dict]:
        """过滤信号"""
        market_score = market_env['score']
        
        filtered = []
        for sig in signals:
            # 弱势市场提高标准
            min_score = 8 if market_score < 0.4 else 5
            
            if sig['direction'] == '推荐' and sig['composite'] >= min_score:
                # 检查置信度
                if market_score < 0.4:
                    if sig['confidence'] >= 0.7:
                        filtered.append(sig)
                else:
                    filtered.append(sig)
        
        return filtered
    
    def generate_report(self, signals: List[Dict], market_env: Dict, date: str) -> Dict:
        """生成分析报告"""
        report = {
            'date': date,
            'market_environment': market_env,
            'total_analyzed': len(signals),
            'recommendations': [],
            'warnings': [],
            'summary': {}
        }
        
        # 按评分排序
        signals.sort(key=lambda x: -x['composite'])
        
        # 强烈推荐
        strong = [s for s in signals if s['composite'] >= 10]
        # 谨慎推荐
        cautious = [s for s in signals if 5 <= s['composite'] < 10]
        # 规避
        avoid = [s for s in signals if s['composite'] < -5]
        
        report['summary'] = {
            'strong_buy': len(strong),
            'cautious_buy': len(cautious),
            'avoid': len(avoid)
        }
        
        # 强烈推荐TOP10
        for s in strong[:10]:
            report['recommendations'].append({
                'rank': len(report['recommendations']) + 1,
                'code': s['code'],
                'name': s['name'],
                'price': s['price'],
                'pct': s['pct'],
                'rsi': s['rsi'],
                'vr': s['vr'],
                'score': s['composite'],
                'reasons': s['v3_reasons']
            })
        
        # 高位风险警告
        high_risk = [s for s in signals if s['rsi'] > 70 and s['vr'] > 1.5]
        for s in high_risk[:5]:
            report['warnings'].append({
                'code': s['code'],
                'name': s['name'],
                'rsi': s['rsi'],
                'vr': s['vr'],
                'reason': '高位放量风险'
            })
        
        return report


def main():
    print("=" * 70)
    print("V4系统 - V38.2 + 经验规则V3 + 市场环境自适应")
    print("=" * 70)
    
    analyzer = V4Analyzer()
    date = datetime.now().strftime('%Y-%m-%d')
    
    # 1. 获取市场环境
    print("\n【1. 市场环境分析】")
    market_env = analyzer.get_market_environment()
    print(f"  市场评分: {market_env['score']:.2f}")
    print(f"  市场状态: {market_env['condition']}")
    print(f"  特征: 指数{market_env['features'].get('index_change', 0):+.2f}%")
    
    # 2. 获取股票池
    print("\n【2. 获取股票池】")
    stocks = analyzer.get_all_stocks()
    print(f"  获取到 {len(stocks)} 只股票")
    
    # 3. 分析所有股票
    print("\n【3. 股票分析】")
    signals = analyzer.analyze_stocks(stocks, market_env)
    print(f"  分析完成: {len(signals)} 只")
    
    # 4. 过滤信号
    print("\n【4. 信号过滤】")
    filtered = analyzer.filter_signals(signals, market_env)
    print(f"  过滤后: {len(filtered)} 只")
    
    # 5. 生成报告
    print("\n【5. 生成报告】")
    report = analyzer.generate_report(signals, market_env, date)
    
    # 6. 显示结果
    print("\n" + "=" * 70)
    print(f"【{date} V4系统分析报告】")
    print("=" * 70)
    
    print(f"\n市场环境: {market_env['condition']} (评分: {market_env['score']:.2f})")
    print(f"分析股票: {report['total_analyzed']}只")
    print(f"强烈推荐: {report['summary']['strong_buy']}只")
    print(f"谨慎推荐: {report['summary']['cautious_buy']}只")
    print(f"建议规避: {report['summary']['avoid']}只")
    
    print("\n" + "-" * 70)
    print("【强烈推荐 TOP 10】")
    print("-" * 70)
    print(f"{'排名':<4} {'股票':<10} {'价格':<8} {'涨幅':<8} {'RSI':<5} {'量比':<6} {'评分':<6} {'理由'}")
    print("-" * 70)
    
    for rec in report['recommendations'][:10]:
        reason = rec['reasons'][0][:12] if rec['reasons'] else ''
        print(f"{rec['rank']:<4} {rec['name']:<10} {rec['price']:<8.2f} {rec['pct']:>+6.2f}% {rec['rsi']:<5.0f} {rec['vr']:<6.2f} {rec['score']:<+6.1f} {reason}")
    
    if report['warnings']:
        print("\n" + "-" * 70)
        print("【高位风险警告】")
        print("-" * 70)
        for w in report['warnings']:
            print(f"  {w['name']}({w['code']}) RSI:{w['rsi']:.0f} 量比:{w['vr']:.2f} - {w['reason']}")
    
    # 7. 保存结果
    output_file = f'/mnt/d/AStockV4/predictions/v4_analysis_{date.replace("-", "")}.json'
    with open(output_file, 'w') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    print(f"\n报告已保存到: {output_file}")
    
    return report


if __name__ == '__main__':
    main()
