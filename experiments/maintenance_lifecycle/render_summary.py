"""Render analysis cells without reinterpreting raw samples or rounding ratios early."""
import json
from pathlib import Path
import sys

result=json.loads(Path(sys.argv[1]).read_text())
print('| 格式 | 每次更新后 q | FS-scan 总秒数 | 增量 FS-index 总秒数 | IoTDB-FS 总秒数 |')
print('| --- | ---: | ---: | ---: | ---: |')
for cell in result['cells']:
    values=[cell['arms'][a]['total_seconds'] for a in ('FS-scan','FS-index','IoTDB-FS')]
    print('| '+cell['format']+' | '+str(cell['q'])+' | '+' | '.join(f"{v['mean']:.3f} [{v['min']:.3f}, {v['max']:.3f}]" for v in values)+' |')
print('\n表中为三轮完整生命周期的算术均值 [最小值, 最大值]；原始三轮值、中位数与分阶段统计保留在 analysis.json。')
print('\n| 格式/q | 路径 | 初始化 ms | 初始8次发布 ms | 16次更新 ms | 查询总和 ms | 收尾 ms |')
print('| --- | --- | ---: | ---: | ---: | ---: | ---: |')
for cell in result['cells']:
    for a,v in cell['arms'].items():
        print('| '+cell['format']+'/'+str(cell['q'])+' | '+a+' | '+' | '.join(f"{v[k]['mean']:.2f}" for k in ('initialization_ms','initial_publications_ms','updates_ms','query_sum_ms','finish_ms'))+' |')
print('\n阶段值是各轮阶段总时长的均值，可与总时钟比较；两者差额含循环及轻量实验记录，不通过各阶段中位数相加生成总时间。')
