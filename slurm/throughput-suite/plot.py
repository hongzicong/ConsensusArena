#!/usr/bin/env python3
"""Plot direct achieved throughput, retaining every attempt and timeout.

Usage: python plot.py SUITE_ROOT --output FIGURE_DIRECTORY
Solid curves use the search-selected, fully drained points in offered-rate order.
All measured attempts are overlaid; x marks denote timeout/unknown completeness.
No monotonicity filter or latency-based deletion is applied. A comparison panel
also shows completion rate against offered rate. CSVs retain invalid-run errors.
"""
import argparse
import csv
import json
from pathlib import Path
import statistics

LABELS = {'agreement':'Agreement','paxos':'Paxos','swiftpaxos':'SwiftPaxos',
          'bodega':'Bodega','kcensus':'KCensus','pqlfrontier':'PQL (frontier)'}


def plot(root, output, allow_partial=False):
    manifest = json.loads((root/'suite.json').read_text())
    status = json.loads((root/'suite-status.json').read_text())
    if not status['complete'] and not allow_partial:
        raise ValueError('incomplete search; use --allow-partial to label a diagnostic plot')
    with (root/'attempts.csv').open(newline='') as f:
        rows = list(csv.DictReader(f))
    if not rows:
        raise ValueError('no observations')
    protocols = manifest['protocols']
    for row in rows:
        if row['protocol'] not in protocols or row['profile'] != 'B' or row['binary_sha256'] != manifest['binary_sha256']:
            raise ValueError('mixed measurement identity')
    if 'sweep' in manifest['spec']:
        return plot_sweep(output, manifest, status, rows, allow_partial)
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D
    output.mkdir(parents=True, exist_ok=True)
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.5))
    for i, protocol in enumerate(protocols):
        color = f'C{i}'
        measured = [r for r in rows if r['protocol']==protocol and r['valid']=='True' and r['mean_ms'] != '']
        selected = sorted((r for r in measured if r['selected']=='True' and r['all_requests_drained']=='True'),
                          key=lambda r: int(r['offered_rps']))
        axes[0].plot([float(r['achieved_rps']) for r in selected], [float(r['mean_ms']) for r in selected],
                     color=color, label=LABELS[protocol], linewidth=1.4)
        axes[1].plot([int(r['offered_rps']) for r in selected], [float(r['achieved_rps']) for r in selected], color=color)
        for r in measured:
            marker = 'x' if r['timed_out']=='True' or r['cohort_complete']!='True' else 'o'
            alpha = 1 if r['selected']=='True' else .4
            axes[0].scatter(float(r['achieved_rps']), float(r['mean_ms']), marker=marker, s=24, color=color, alpha=alpha)
            axes[1].scatter(int(r['offered_rps']), float(r['achieved_rps']), marker=marker, s=24, color=color, alpha=alpha)
    axes[0].axhline(manifest['spec']['search']['latency_cap_ms'], linestyle=':', color='gray', linewidth=1)
    axes[0].set(xlabel='Achieved throughput (requests/s)', ylabel='Mean completed-request latency (ms)')
    ceiling = max(int(r['offered_rps']) for r in rows)
    axes[1].plot([0,ceiling],[0,ceiling],linestyle=':',color='gray',linewidth=1)
    axes[1].set(xlabel='Offered load (requests/s)', ylabel='Completed requests/s during measurement')
    for ax in axes:
        ax.grid(alpha=.25)
        ax.set_xlim(left=0)
        ax.set_ylim(bottom=0)
    handles, labels = axes[0].get_legend_handles_labels()
    handles.append(Line2D([],[],color='black',marker='x',linestyle='None'))
    labels.append('Timeout/incomplete cohort')
    fig.legend(handles,labels,loc='upper center',ncol=4,fontsize=9)
    caption = 'ring-7 · B (95% reads, Zipf 0.99) · direct 10s completion counts · faint points: other attempts'
    if not status['complete']:
        caption = 'INCOMPLETE SEARCH · '+caption
    fig.text(.5,.015,caption,ha='center',fontsize=8.5)
    fig.tight_layout(rect=(0,.07,1,.85))
    for ext in ('png','pdf'):
        fig.savefig(output/f'throughput.{ext}',dpi=200)
    plt.close(fig)


def plot_sweep(output, manifest, status, rows, allow_partial=False):
    """Show every repetition; summarize complete groups without selecting successes."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D
    controls = manifest['spec']['sweep']
    output.mkdir(parents=True, exist_ok=True)
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.8))
    summaries = []
    for protocol in manifest['protocols']:
        color = 'C'+str(list(LABELS).index(protocol))
        measured = [r for r in rows if r['protocol']==protocol and r['valid']=='True' and r['mean_ms']]
        offered_means = []
        for rate in controls['rates']:
            group = [r for r in measured if int(r['offered_rps'])==rate]
            drained = [r for r in group if r['all_requests_drained']=='True' and r['timed_out']=='False' and r['cohort_complete']=='True']
            for r in group:
                marker = 'o' if r in drained else 'x'
                for ax, x in zip(axes, (float(r['achieved_rps']), rate)):
                    ax.scatter(x, float(r['mean_ms']), color=color, marker=marker, s=19, alpha=.4)
            # An incomplete repetition must not disappear inside a success-only mean.
            complete = len(drained)==controls['repetitions'] and len(group)==controls['repetitions']
            if complete:
                x = [float(r['achieved_rps']) for r in group]
                y = [float(r['mean_ms']) for r in group]
                mx, my = statistics.mean(x), statistics.mean(y)
                for ax, center, xerr in ((axes[0], mx, [[mx-min(x)], [max(x)-mx]]), (axes[1], rate, None)):
                    ax.errorbar(center, my, xerr=xerr, yerr=[[my-min(y)], [max(y)-my]],
                                fmt='o', color=color, capsize=2, markersize=4)
                offered_means.append(my)
            else:
                offered_means.append(float('nan'))
            summaries.append(dict(protocol=protocol, offered_rps=rate, measured_repetitions=len(group),
                fully_drained_repetitions=len(drained), aggregate_complete=complete,
                achieved_mean_rps=statistics.mean(float(r['achieved_rps']) for r in group) if complete else '',
                mean_latency_ms=statistics.mean(float(r['mean_ms']) for r in group) if complete else ''))
        # Offered rate has a well-defined order; achieved-rate means are shown as points.
        axes[1].plot(controls['rates'], offered_means, color=color, label=LABELS[protocol], linewidth=1)
    for ax in axes:
        ax.axhline(controls['latency_cap_ms'], linestyle=':', color='gray', linewidth=1)
        ax.grid(alpha=.25)
        ax.set(xlim=(0, None), ylim=(0, None), ylabel='Mean completed-request latency (ms)')
    axes[0].set_xlabel('Achieved throughput (requests/s)')
    axes[1].set_xlabel('Offered load (requests/s)')
    handles = [Line2D([], [], color='C'+str(list(LABELS).index(p)), marker='o', linestyle='None', label=LABELS[p]) for p in manifest['protocols']]
    handles.append(Line2D([], [], color='black', marker='x', linestyle='None', label='Timeout/incomplete cohort'))
    fig.legend(handles=handles, loc='upper center', ncol=3, fontsize=9)
    caption = 'ring-7 · B · 10s measurement · all repetitions retained · mean and min–max bars only when every repetition drains'
    if not status['complete']:
        caption = 'INCOMPLETE SWEEP · '+caption
    fig.text(.5, .015, caption, ha='center', fontsize=8)
    fig.tight_layout(rect=(0, .06, 1, .88))
    with (output/'sweep-summary.csv').open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(summaries[0]))
        writer.writeheader()
        writer.writerows(summaries)
    for ext in ('png', 'pdf'):
        fig.savefig(output/f'throughput.{ext}', dpi=200)
    plt.close(fig)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root',type=Path)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--allow-partial',action='store_true')
    args = parser.parse_args()
    plot(args.root,args.output,args.allow_partial)
