"""Generate a protocol package without modifying existing packages or registration."""
import argparse
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
KEYWORDS = set('break default func interface select case defer go map struct chan else goto package switch const fallthrough if range type continue for import return var'.split())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('name', help='Go package name, e.g. xpaxos')
    parser.add_argument('--output', help='Optional destination relative to ConsensusArena')
    args = parser.parse_args()
    if not re.fullmatch(r'[a-z][a-z0-9_]*', args.name) or args.name in KEYWORDS:
        parser.error('name must be a lowercase Go identifier and not a keyword')
    target = (ROOT / (args.output or args.name)).resolve()
    if not target.is_relative_to(ROOT) or target == ROOT:
        parser.error('destination must be inside ConsensusArena')
    if target.exists():
        parser.error('destination already exists; no files were changed')
    # Format every template before creating the destination, so missing gofmt or
    # a malformed template cannot leave a partially generated package.
    files = {}
    for source in sorted((ROOT / 'templates/protocol').glob('*.tmpl')):
        text = source.read_text(encoding='utf-8').replace('__PACKAGE__', args.name)
        name = source.name.removesuffix('.tmpl')
        if name.endswith('.go'):
            text = subprocess.run(['gofmt'], input=text, text=True, capture_output=True, check=True).stdout
        files[name] = text
    target.mkdir(parents=True, exist_ok=False)
    for name, text in files.items():
        (target / name).write_text(text, encoding='utf-8', newline='\n')
    print(f'Created {target}; protocol remains unregistered and refuses startup until implemented.')


if __name__ == '__main__':
    main()
