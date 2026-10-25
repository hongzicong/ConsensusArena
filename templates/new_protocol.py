"""Generate a protocol package without modifying existing packages or registration."""
import argparse
import importlib.util
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
KEYWORDS = set('break default func interface select case defer go map struct chan else goto package switch const fallthrough if range type continue for import return var'.split())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('name', help='Go package name, e.g. xpaxos')
    parser.add_argument('--output', help='Optional destination relative to ConsensusArena')
    parser.add_argument('--plugin', help='Path to protoc-gen-gogo v1.7.2; otherwise use PATH')
    args = parser.parse_args()
    if not re.fullmatch(r'[a-z][a-z0-9_]*', args.name) or args.name in KEYWORDS:
        parser.error('name must be a lowercase Go identifier and not a keyword')
    target = (ROOT / (args.output or args.name)).resolve()
    if not target.is_relative_to(ROOT) or target == ROOT:
        parser.error('destination must be inside ConsensusArena')
    if target.exists():
        parser.error('destination already exists; no files were changed')
    relative=target.relative_to(ROOT)
    if any(not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',part) for part in relative.parts):
        parser.error('destination components must be identifiers (letters, digits and underscores)')
    # Format every template before creating the destination, so missing gofmt or
    # a malformed template cannot leave a partially generated package.
    files = {}
    for source in sorted((ROOT / 'templates/protocol').glob('*.tmpl')):
        text = (source.read_text(encoding='utf-8').replace('__PACKAGE__', args.name)
                .replace('__IMPORT_PATH__', 'github.com/hongzicong/ConsensusArena/'+relative.as_posix())
                .replace('__PROTO_PACKAGE__', '.'.join(relative.parts))
                .replace('__PACKAGE_PATH__', relative.as_posix())
                .replace('__GENERATOR_PATH__', Path(os.path.relpath(ROOT/'wireproto/generate.py',target)).as_posix()))
        name = source.name.removesuffix('.tmpl')
        if name.endswith('.go'):
            text = subprocess.run(['gofmt'], input=text, text=True, capture_output=True, check=True).stdout
        files[name] = text
    # Generate the final schema path in a private staging tree. Shared imports
    # resolve from ROOT without regenerating any baseline files.
    sys.dont_write_bytecode=True
    spec=importlib.util.spec_from_file_location('arena_wire_generator',ROOT/'wireproto/generate.py')
    generator=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(generator)
    scratch=ROOT/'results'
    scratch.mkdir(exist_ok=True)
    if not scratch.resolve().is_relative_to(ROOT):
        parser.error('staging directory must stay inside ConsensusArena')
    try:
        with tempfile.TemporaryDirectory(prefix='new-protocol-',dir=scratch) as temporary:
            stage=Path(temporary).resolve()
            assert stage.is_relative_to(scratch.resolve())
            staged_target=stage/relative
            staged_target.mkdir(parents=True)
            for name, text in files.items():
                (staged_target/name).write_text(text,encoding='utf-8',newline='\n')
            generator.generate([relative.as_posix()],source_root=stage,output_root=stage,plugin=args.plugin)
            target.parent.mkdir(parents=True,exist_ok=True)
            # Both paths stay inside this repository. Refuse a destination that
            # appeared while protoc was running before moving the completed tree.
            if target.exists():raise FileExistsError('destination already exists')
            staged_target.rename(target)
    except (ValueError,OSError,subprocess.CalledProcessError,RuntimeError) as exc:
        parser.exit(1,str(exc)+'\n')
    print(f'Created {target}; protocol remains unregistered and refuses startup until implemented.')


if __name__ == '__main__':
    main()
