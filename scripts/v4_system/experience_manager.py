"""
B+C 统一因子与经验库管理系统
=====================================
B系统（本地）和C系统（服务器）共享同一个经验库

经验库结构:
{
  "factors": {
    "IC排序的数值因子": {
      "vr_10": {"ic": -0.055, "spread": -0.094, "samples": 43600, "updated": "2026-04-29"},
      "pct_1d": {"ic": -0.052, "spread": -0.178, "samples": 43600, "updated": "2026-04-29"},
      ...
    }
  },
  "rules": {
    "组合规则（高置信度）": {
      "RSI<40+VR>1.2+跌幅>5%": {"accuracy": 73.5, "samples": 500, "updated": "2026-04-29"},
      ...
    }
  },
  "metadata": {
    "last_sync": "2026-04-29",
    "total_samples": 43600,
    "version": "v4.7"
  }
}

存储位置:
- 本地: /mnt/d/AStockV4/predictions/experience/unified_experience_db.json
- GitHub: fengzhancheng12345/AStockV4-Systems/predictions/experience_db.json
"""

import json
import os
from datetime import datetime

EXPERIENCE_DIR = '/mnt/d/AStockV4/predictions/experience'
GITHUB_REPO = 'fengzhancheng12345/AStockV4-Systems'

def get_experience_path():
    os.makedirs(EXPERIENCE_DIR, exist_ok=True)
    return f'{EXPERIENCE_DIR}/unified_experience_db.json'

def load_experience():
    """加载经验库"""
    path = get_experience_path()
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {'factors': {}, 'rules': {}, 'metadata': {'version': 'v4.7', 'created': datetime.now().strftime('%Y-%m-%d')}}

def save_experience(db):
    """保存经验库"""
    path = get_experience_path()
    db['metadata']['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(db, f, ensure_ascii=False, indent=2)

def add_factor_result(name, ic, spread, samples=0):
    """添加数值因子测试结果"""
    db = load_experience()
    if 'factors' not in db:
        db['factors'] = {}
    db['factors'][name] = {
        'ic': round(ic, 6),
        'ic_abs': round(abs(ic), 6),
        'spread': round(spread, 4),
        'samples': samples,
        'updated': datetime.now().strftime('%Y-%m-%d')
    }
    db['metadata']['total_samples'] = db['metadata'].get('total_samples', 0) + samples
    save_experience(db)
    print(f"Added factor: {name} IC={ic:.4f}")

def add_rule(text, accuracy, samples=1):
    """添加组合规则"""
    db = load_experience()
    if 'rules' not in db:
        db['rules'] = {}
    if text in db['rules']:
        r = db['rules'][text]
        new_total = r['samples'] + samples
        r['accuracy'] = (r['accuracy'] * r['samples'] + accuracy * samples) / new_total
        r['samples'] = new_total
    else:
        db['rules'][text] = {
            'accuracy': accuracy,
            'samples': samples
        }
    db['rules'][text]['updated'] = datetime.now().strftime('%Y-%m-%d')
    save_experience(db)

def get_top_factors(n=20, min_ic=0.01):
    """获取Top N因子"""
    db = load_experience()
    factors = db.get('factors', {})
    sorted_factors = sorted(factors.items(), key=lambda x: -x[1].get('ic_abs', 0))
    return [(name, info) for name, info in sorted_factors if info.get('ic_abs', 0) >= min_ic][:n]

def get_top_rules(n=20, min_accuracy=50):
    """获取Top N规则"""
    db = load_experience()
    rules = db.get('rules', {})
    sorted_rules = sorted(rules.items(), key=lambda x: -x[1].get('accuracy', 0))
    return [(text, info) for text, info in sorted_rules if info.get('accuracy', 0) >= min_accuracy][:n]

def merge_results(factor_results_path):
    """合并B系统测试结果到经验库"""
    if not os.path.exists(factor_results_path):
        print(f"File not found: {factor_results_path}")
        return
    with open(factor_results_path, 'r', encoding='utf-8') as f:
        results = json.load(f)
    for r in results:
        add_factor_result(r['name'], r['ic'], r.get('spread', 0), r.get('samples', 0))
    print(f"Merged {len(results)} factor results")

if __name__ == '__main__':
    # 测试：查看当前经验库
    db = load_experience()
    print(f"Experience DB: {len(db.get('factors', {}))} factors, {len(db.get('rules', {}))} rules")
    print(f"Metadata: {db.get('metadata', {})}")
    
    # 显示Top因子
    print("\nTop 10 Factors:")
    for name, info in get_top_factors(10):
        print(f"  {name}: IC={info['ic']:.4f}, Spread={info['spread']:.4f}")
