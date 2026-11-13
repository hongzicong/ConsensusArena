"""Recreate paper figures from the CSVs in this directory. No cluster access required.

    python results/plot.py                 # all three experiments
    python results/plot.py scalability     # one experiment
    python results/plot.py --full-range    # do not cap the traffic/message axes

Missing measurements remain missing. Plot eligibility is recorded in the CSVs.
"""
import argparse
import csv
import math
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from matplotlib.ticker import MaxNLocator, StrMethodFormatter

ROOT = Path(__file__).resolve().parent
PROTOCOLS = ('agreement', 'paxos', 'swiftpaxos', 'bodega', 'kcensus', 'pqlfrontier')
LABELS = ('Agreement', 'Paxos', 'SwiftPaxos', 'Bodega', 'KCensus', 'PQL')
COLORS = ('#0072B2', '#707070', '#8064A2', '#009E73', '#D55E00', '#56B4E9')
MARKERS = ('o', 's', 'D', '^', 'v', 'P')
DEPLOYMENTS = (('ring-7', 'Worldwide ring · 7 replicas'),
               ('europe-7', 'Europe · 7 replicas'),
               ('east-asia-7', 'East Asia · 7 replicas'),
               ('north-america-7', 'North America · 7 replicas'),
               ('north-america-3', 'North America · 3 replicas'),
               ('north-america-5', 'North America · 5 replicas'))

plt.rcParams.update({'font.size': 10, 'axes.titlesize': 11, 'axes.labelsize': 10,
    'axes.spines.top': False, 'axes.spines.right': False, 'axes.axisbelow': True,
    'font.family': 'DejaVu Sans', 'pdf.fonttype': 42, 'ps.fonttype': 42,
    'savefig.facecolor': 'white', 'figure.facecolor': 'white'})

def table(suite, name):
    with (ROOT/suite/'data'/name).open(newline='', encoding='utf-8') as source:
        return list(csv.DictReader(source))

def number(row, field):
    value = row.get(field, '')
    return float(value) if value else math.nan

def save(fig, suite, name, output):
    folder = output/suite if output else ROOT/suite/'figures'
    folder.mkdir(parents=True, exist_ok=True)
    for extension in ('pdf', 'png'):
        fig.savefig(folder/(name+'.'+extension), dpi=200, bbox_inches='tight')
    plt.close(fig)
    print(folder/(name+'.pdf'))

def protocol_legend(fig, top=.98):
    handles = [Line2D([], [], color=color, marker=marker, label=label,
                      markersize=4, linewidth=1.6)
               for label,color,marker in zip(LABELS,COLORS,MARKERS)]
    fig.legend(handles=handles, loc='upper center', bbox_to_anchor=(.5,top),
               ncol=6, frameon=False, columnspacing=1.1, handlelength=1.6, fontsize=9)

def regular(output):
    rows = table('regular', 'latency.csv')
    lookup = {(r['deployment'],r['profile'],r['protocol']):r for r in rows}
    if len(rows)!=72 or len(lookup)!=72:
        raise ValueError('Regular CSV must contain all 72 unique configurations')
    for metric, title, filename in [('mean_ms','Mean request latency (ms)','mean-latency'),
                                     ('p99_ms','p99 request latency (ms)','p99-latency')]:
        fig, axes = plt.subplots(2, 3, figsize=(12.5, 6.4))
        for ax,(deployment,label) in zip(axes.flat,DEPLOYMENTS):
            for i,(protocol,color) in enumerate(zip(PROTOCOLS,COLORS)):
                for profile,shift,hatch,alpha in [('A',-.19,'',1),('B',.19,'///',.65)]:
                    row = lookup[deployment,profile,protocol]
                    y = number(row,metric) if row['plot_eligible']=='True' else math.nan
                    ax.bar(i+shift,y,width=.35,color=color,alpha=alpha,hatch=hatch,
                           edgecolor='white',linewidth=.5)
            ax.set(title=label, xticks=range(6), xticklabels=LABELS, ylabel=title, ylim=(0,None))
            ax.tick_params(axis='x',labelsize=8,rotation=25)
            ax.grid(axis='y',alpha=.2)
            ax.yaxis.set_major_locator(MaxNLocator(5))
        fig.legend(handles=[Patch(facecolor='#505050',label='A · 50% writes'),
                            Patch(facecolor='#a0a0a0',hatch='///',label='B · 5% writes')],
                   loc='upper center',ncol=2,frameon=False)
        fig.text(.5,.01,'20,000 offered requests/s · one run per configuration · complete, fully drained cohorts\n'
                 'Each topology is shown separately; no averaging across topologies.',ha='center',fontsize=9)
        fig.tight_layout(rect=(0,.07,1,.94),h_pad=1.5,w_pad=1.5)
        save(fig,'regular',filename,output)

def scaling_curve(ax, rows, metric, label, cap=None):
    clipped = 0
    for protocol,color,marker in zip(PROTOCOLS,COLORS,MARKERS):
        series = sorted((r for r in rows if r['protocol']==protocol),key=lambda r:int(r['replicas']))
        xs = [int(r['replicas']) for r in series]
        ys = [number(r,metric) if r['plot_eligible']=='True' else math.nan for r in series]
        ax.plot(xs,ys,color=color,marker=marker,markersize=4,linewidth=1.8 if protocol=='agreement' else 1.25)
        valid = [i for i,y in enumerate(ys) if math.isfinite(y)]
        for a,b in zip(valid,valid[1:]):
            if b>a+1:
                ax.plot([xs[a],xs[b]],[ys[a],ys[b]],color=color,linestyle=':',linewidth=1.2)
        if cap:
            outside = [i for i,y in enumerate(ys) if math.isfinite(y) and y>cap]
            clipped += len(outside)
            ax.scatter([xs[i] for i in outside],[cap*.985]*len(outside),color=color,marker='^',s=25,zorder=5)
    ax.set(xlabel='Number of replicas',ylabel=label,xlim=(2,32),ylim=(0,cap),xticks=list(range(3,32,4)))
    ax.grid(alpha=.2)
    ax.yaxis.set_major_locator(MaxNLocator(6))
    if clipped:
        ax.text(.02,.97,f'▲ {clipped} points exceed axis limit',transform=ax.transAxes,va='top',fontsize=8,
                bbox=dict(facecolor='white',alpha=.85,edgecolor='none'))
    return clipped

def scalability(output, full_range):
    rows=table('scalability','scalability.csv')
    expected={(p,n) for p in PROTOCOLS for n in range(3,32,2)}
    if {(r['protocol'],int(r['replicas'])) for r in rows}!=expected or len(rows)!=90:
        raise ValueError('Scalability CSV must retain all 90 configurations, including failures')
    metrics=[('mean_ms','Mean request latency (ms)','mean-latency',None),
             ('p99_ms','p99 request latency (ms)','p99-latency',None),
             ('bytes_per_request_per_replica','Bytes/request/replica','bytes',None if full_range else 750),
             ('messages_per_request_per_replica','Messages/request/replica','messages',None if full_range else 6),
             ('cpu_percent_per_replica','CPU per replica (% of one logical CPU)','cpu',100),
             ('peak_memory_mib_per_replica','Peak memory per replica (MiB)','peak-memory',None)]
    note='B · AWS-random · 20,000 offered requests/s · complete, fully drained runs only\n'
    note+='SwiftPaxos omitted: 21–27 replicas timed out; 29–31 replicas ran out of memory during startup.'
    for metric,label,name,cap in metrics:
        fig,ax=plt.subplots(figsize=(7,4.3))
        scaling_curve(ax,rows,metric,label,cap)
        protocol_legend(fig)
        caption=note
        if name=='peak-memory':
            caption+='\nMean of replica lifetime kernel peaks through measurement end; includes startup and warmup.'
        elif name in ('bytes','messages'):
            caption+='\nApplication send + receive counters / measured system completions / replicas; clipped values remain in CSV.'
        fig.text(.5,.005,caption,ha='center',fontsize=7.8)
        fig.tight_layout(rect=(0,.13 if name in ('peak-memory','bytes','messages') else .10,1,.90))
        save(fig,'scalability',name,output)
    fig,axes=plt.subplots(2,2,figsize=(11,7))
    for ax,(metric,label,name,cap) in zip(axes.flat,metrics[2:]):
        scaling_curve(ax,rows,metric,label,cap)
    protocol_legend(fig)
    fig.text(.5,.005,note+'\nMemory: mean of replica kernel peaks, including startup/warmup. ▲ exceeds axis limit; full values remain in CSV.',
             ha='center',fontsize=8)
    fig.tight_layout(rect=(0,.09,1,.93),h_pad=1.5,w_pad=2)
    save(fig,'scalability','resources',output)

def throughput(output):
    rows=table('throughput','points.csv')
    attempts=table('throughput','attempts.csv')
    if len(rows)!=110 or len(attempts)!=133:
        raise ValueError('Expected 110 selected throughput points and 133 total attempts')
    fig,axes=plt.subplots(1,2,figsize=(11.5,4.7))
    for protocol,color,marker in zip(PROTOCOLS,COLORS,MARKERS):
        series=sorted((r for r in rows if r['protocol']==protocol),key=lambda r:int(r['offered_rps']))
        valid=[r for r in series if r['throughput_plot_eligible']=='True']
        axes[0].plot([number(r,'offered_rps')/1000 for r in valid],
                     [number(r,'achieved_rps')/1000 for r in valid],color=color,marker=marker,markersize=4,linewidth=1.4)
        incomplete=[r for r in valid if r['latency_plot_eligible']!='True']
        axes[0].scatter([number(r,'offered_rps')/1000 for r in incomplete],
                        [number(r,'achieved_rps')/1000 for r in incomplete],color=color,marker='x',s=50,zorder=5)
        # NaNs break the curve at excluded latency observations; survivor means are not plotted.
        axes[1].plot([number(r,'achieved_rps')/1000 for r in series],
                     [number(r,'mean_ms') if r['latency_plot_eligible']=='True' else math.nan for r in series],
                     color=color,marker=marker,markersize=4,linewidth=1.4)
    maximum=max(number(r,'offered_rps') for r in rows)/1000
    axes[0].plot([0,maximum],[0,maximum],color='#888888',linestyle=':',linewidth=1)
    axes[0].set(xlabel='Offered load (thousand requests/s)',ylabel='Achieved throughput (thousand requests/s)')
    axes[1].axhline(400,color='#888888',linestyle=':',linewidth=1)
    axes[1].text(.99,400,'400 ms search cap',transform=axes[1].get_yaxis_transform(),ha='right',va='bottom',fontsize=8)
    axes[1].set(xlabel='Achieved throughput (thousand requests/s)',ylabel='Mean request latency (ms)')
    for ax in axes:
        ax.set(xlim=(0,None),ylim=(0,None))
        ax.grid(alpha=.2)
        ax.xaxis.set_major_locator(MaxNLocator(6))
        ax.xaxis.set_major_formatter(StrMethodFormatter('{x:g}'))
    protocol_legend(fig)
    fig.text(.5,.005,'Ring 7 · B (5% writes) · one adaptive search per protocol · selected attempts in offered-load order\n'
        'Throughput: direct completions during [5,15) / 10. × incomplete cohort (rate remains measurable); latency panel omits these runs.\n'
        'All 133 attempts, including retries and incomplete observations, remain in attempts.csv.',ha='center',fontsize=8)
    fig.tight_layout(rect=(0,.11,1,.90),w_pad=2)
    save(fig,'throughput','throughput',output)

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('suite',nargs='?',choices=('all','regular','scalability','throughput'),default='all')
    parser.add_argument('--full-range',action='store_true',help='show full traffic and message ranges')
    parser.add_argument('--output',type=Path,help='alternate output root; figures go into a subdirectory per experiment')
    args=parser.parse_args()
    if args.suite in ('all','regular'): regular(args.output)
    if args.suite in ('all','scalability'): scalability(args.output,args.full_range)
    if args.suite in ('all','throughput'): throughput(args.output)

if __name__=='__main__':
    main()
