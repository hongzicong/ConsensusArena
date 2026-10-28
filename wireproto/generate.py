"""Regenerate native protocol message types and stream glue from package schemas.

Requires protoc, Python 3 and protoc-gen-gogo from cosmos/gogoproto v1.7.2.
No protocol-to-protobuf conversion types or tests are generated.
"""
import argparse
import copy
import json
from pathlib import Path
import re
import shutil
import subprocess

ROOT=Path(__file__).resolve().parents[1]
HERE=ROOT/'wireproto'
PREFIX='github.com/hongzicong/ConsensusArena/'
PACKAGES=['state','replica/defs','paxos','curp','epaxos','swift','fastpaxos','kcensus','bodega']
BASE_MANIFEST=json.loads((HERE/'manifest.json').read_text(encoding='utf-8'))
MANIFEST=copy.deepcopy(BASE_MANIFEST)
SOURCE_ROOT=ROOT
OUTPUT_ROOT=ROOT
SCHEMAS={}
EXTENSIONS=set()
SCALAR_TYPES={'bool','string','bytes','double','float','int32','int64','uint32','uint64','sint32','sint64','fixed32','fixed64','sfixed32','sfixed64'}
LOCAL_FIELDS={('kcensus','Message'):'References bool // Local encoding policy; never transmitted.',('epaxos','PrepareReply'):'VBallot int32 // Local field, intentionally absent from the wire.'}


def messages(pkg):
    source=(SOURCE_ROOT/pkg/'messages.proto').read_text(encoding='utf-8')
    out={}
    for match in re.finditer(r'message (\w+) \{(.*?)\n\}',source,re.S):
        fields=[]
        for f in re.finditer(r'(optional|repeated) ([\w.]+) (\w+) = (\d+)([^;]*);',match[2]):
            custom=re.search(r'customname\)\s*=\s*"([^"]+)"',f[5])
            cast=re.search(r'casttype\)\s*=\s*"([^"]+)"',f[5])
            fields.append(dict(name=custom[1] if custom else ''.join(s.title() for s in f[3].split('_')),number=int(f[4]),type=f[2],repeated=f[1]=='repeated',pointer=f[2][0].isupper() and 'nullable) = false' not in f[5],cast=cast[1] if cast else None,hash='wiretypes.Hash' in f[5]))
        out[match[1]]=fields
    return out

def package_path(value, root):
    path=(root/value).resolve()
    if not path.is_relative_to(root) or path==root:
        raise ValueError('package must be a directory inside the source root')
    if not (path/'messages.proto').is_file():
        raise ValueError(f'{value}: messages.proto not found')
    return path.relative_to(root).as_posix()


def extension_config(pkg):
    """Extend metadata only for an explicitly selected, independent package."""
    source=(SOURCE_ROOT/pkg/'messages.proto').read_text(encoding='utf-8')
    source=re.sub(r'/\*.*?\*/|//[^\n]*','',source,flags=re.S)
    if not re.search(r'\bsyntax\s*=\s*"proto2"\s*;',source):
        raise ValueError(f'{pkg}: use the template proto2 schema conventions')
    structure=re.sub(r'"(?:\\.|[^"\\])*"','""',source)
    if re.search(r'\b(?:enum|oneof|extend|group)\s+\w+|\bmap\s*<|\brequired\s+|\bextensions\s+',structure):
        raise ValueError(f'{pkg}: enums, oneof, maps, required fields and extensions are not supported by the wire-shape generator')
    depth=0
    for token in re.finditer(r'\bmessage\s+\w+\s*\{|[{}]',structure):
        value=token[0]
        if value.startswith('message'):
            if depth:raise ValueError(f'{pkg}: define messages at top level')
            depth+=1
        elif value=='{':depth+=1
        else:depth-=1
    if any(not re.fullmatch(r'[A-Z][A-Za-z0-9]*',name) for name in SCHEMAS[pkg]):
        raise ValueError(f'{pkg}: use PascalCase message names without underscores')
    path=SOURCE_ROOT/pkg/'wireproto.json'
    if not path.is_file():raise ValueError(f'{pkg}: wireproto.json not found')
    config=json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(config,dict) or set(config)-{'roots','local_names','aliases'}:
        raise ValueError(f'{pkg}: expected roots, local_names and optional aliases')
    roots=config.get('roots')
    if not isinstance(roots,list) or not roots:raise ValueError(f'{pkg}: roots must be a nonempty list')
    seen=set()
    identifier=re.compile(r'[A-Za-z_][A-Za-z0-9_]*')
    for entry in roots:
        if not isinstance(entry,dict) or set(entry)-{'type','model','limit','validate','project'}:
            raise ValueError(f'{pkg}: unsupported stream entry')
        typ=entry.get('type');model=entry.get('model',typ)
        if not isinstance(typ,str) or typ not in SCHEMAS[pkg] or model!=typ or typ in seen:
            raise ValueError(f'{pkg}: each stream must name one unique native generated type')
        seen.add(typ);entry['model']=model
        limit=entry.get('limit',64<<20)
        if type(limit) is not int or not 0<limit<=64<<20:
            raise ValueError(f'{pkg}: frame limit must be 1..67108864 bytes')
        entry['limit']=limit
        validate=entry.get('validate')
        if validate is not None and (not isinstance(validate,str) or not identifier.fullmatch(validate)):
            raise ValueError(f'{pkg}: validate must name a Go function')
        project=entry.get('project')
        if project is not None and (not isinstance(project,str) or not identifier.fullmatch(project)):
            raise ValueError(f'{pkg}: project must name a native-message method')
    names=config.get('local_names',{})
    if not isinstance(names,dict) or any(not isinstance(k,str) or not identifier.fullmatch(k) or k in SCHEMAS[pkg] or not isinstance(v,str) or v not in SCHEMAS[pkg] for k,v in names.items()):
        raise ValueError(f'{pkg}: invalid local message aliases')
    aliases=config.get('aliases',{})
    allowed={'byte','uint8','uint16','int8','int16','int32','int64','uint32','uint64','int','bool','string','[]byte'}
    if not isinstance(aliases,dict) or any(not isinstance(k,str) or not identifier.fullmatch(k) or not isinstance(v,str) or v not in allowed for k,v in aliases.items()):
        raise ValueError(f'{pkg}: aliases must map local Go identifiers to scalar types')
    MANIFEST['roots'][pkg]=roots
    MANIFEST.setdefault('local_names',{})[pkg]=names
    MANIFEST['aliases'].update({pkg+'.'+k:v for k,v in aliases.items()})
    for fields in SCHEMAS[pkg].values():
        for field in fields:
            cast=field['cast']
            short=cast.removeprefix(PREFIX) if cast else None
            base=MANIFEST['aliases'].get(short,cast)
            base=MANIFEST['aliases'].get(pkg+'.'+str(base),base)
            if base in ('int8','int16') and field['type']!='sint32':
                raise ValueError(f'{pkg}: narrow signed casts require sint32 fields')
            if base in ('byte','uint8','uint16') and field['type'] not in ('uint32','uint64'):
                raise ValueError(f'{pkg}: narrow unsigned casts require uint32/uint64 fields')


def generated_functions(source, method):
    for m in re.finditer(r'func \(m \*(\w+)\) '+method+r'\(',source):
        start=source.index('{',m.start());depth=1;end=start+1
        while depth:
            if source[end]=='{':depth+=1
            elif source[end]=='}':depth-=1
            end+=1
        yield m[1],m.start(),end,source[m.start():end]


def patch_generated(pkg):
    path=OUTPUT_ROOT/pkg/'messages.pb.go'
    source=path.read_text(encoding='utf-8')
    # Serializable's stream API is distinct from the generated payload API.
    source=re.sub(r'\bMarshal\(', 'MarshalProto(', source)
    source=re.sub(r'\bUnmarshal\(', 'UnmarshalProto(', source)
    for (p,name),field in LOCAL_FIELDS.items():
        if p==pkg:source=source.replace('type '+name+' struct {','type '+name+' struct {\n\t'+field)
    # Keep the runtime's standard proto.Marshal reference, not a renamed package API.
    source=source.replace('proto.MarshalProto','proto.Marshal')
    source=re.sub(r'(xxx_messageInfo_\w+)\.MarshalProto\(',r'\1.Marshal(',source)
    # Cast types retain narrow protocol fields; ZigZag arithmetic must still
    # run at the schema's 32-bit width before the checked narrowing assignment.
    edits=[]
    for typ,start,end,body in generated_functions(source,'UnmarshalProto'):
        for field in SCHEMAS[pkg][typ]:
            cast=field['cast']
            if cast not in ['int8','int16']:continue
            name=field['name']
            pattern=r'(case '+str(field['number'])+r':.*?)(?=\n\t\tcase |\n\t\tdefault:)'
            def widen(match):
                block=match[1].replace('var v '+cast,'var v int32').replace('v |= '+cast+'(', 'v |= int32(').replace('v = '+cast+'(', 'v = int32(')
                return block.replace('m.'+name+' = v','m.'+name+' = '+cast+'(v)')
            body=re.sub(pattern,widen,body,flags=re.S)
        edits.append((start,end,body))
    for start,end,body in reversed(edits):source=source[:start]+body+source[end:]
    for fields in SCHEMAS[pkg].values():
        for field in fields:
            if field['cast'] in ['int8','int16']:
                name=field['name']
                source=source.replace('('+ 'm.'+name+'>>31)', '(int32(m.'+name+')>>31)')
    # Selection of on-wire fields is policy on the native generated objects.
    # Helpers construct at most a shallow header of the SAME generated type.
    for method in ['Size','MarshalToSizedBuffer']:
        edits=[]
        for typ,start,end,body in generated_functions(source,method):
            if pkg=='kcensus':
                if typ=='Value':
                    body=body.replace('if m.Batch != nil {','if !m.Reference && m.Batch != nil {').replace('if len(m.Records) > 0 {','if !m.Reference && m.Batch == nil && len(m.Records) > 0 {')
                for field,ref in ([('Value','m.References'),('Fast','m.References')] if typ=='Message' else [('Value','true')] if typ=='NodeReport' else []):
                    body=body.replace('m.'+field+'.Size()',f'wireValueSize(m.{field}, {ref})').replace('m.'+field+'.MarshalToSizedBuffer(',f'wireValueMarshal(m.{field}, {ref}, ')
                if typ=='Message':
                    body=body.replace('m.Values[iNdEx].MarshalToSizedBuffer(', 'wireValueMarshal(m.Values[iNdEx], false, ')
                    body=body.replace('for _, e := range m.Values {\n\t\t\tl = e.Size()', 'for _, e := range m.Values {\n\t\t\tl = wireValueSize(e, false)')
            if pkg=='bodega' and typ in ['Message','Entry']:
                # A shallow, native projection keeps field-selection semantics in
                # one place without allocating a second protobuf object tree.
                insertion='\n\twire := m.wireFields()\n\tm = &wire\n'
                pos=body.index('{')+1
                if method=='Size':
                    nil_end=body.index('\n\t}',body.index('if m == nil'))+3
                    body=body[:nil_end]+insertion+body[nil_end:]
                else:body=body[:pos]+insertion+body[pos:]
            if pkg=='fastpaxos' and typ=='Entry':
                body=body.replace('m.Value.Size()', 'wireRecordSize(&m.Value, m.Payload)').replace('m.Value.MarshalToSizedBuffer(', 'wireRecordMarshal(&m.Value, m.Payload, ')
            edits.append((start,end,body))
        for start,end,body in reversed(edits):source=source[:start]+body+source[end:]
    source=source.replace('// Code generated by protoc-gen-gogo. DO NOT EDIT.','// Code generated by protoc-gen-gogo and wireproto/generate.py. DO NOT EDIT.')
    if pkg=='fastpaxos':
        for method in ['Size','MarshalToSizedBuffer']:
            body=next(fn for typ,_,_,fn in generated_functions(source,method) if typ=='Record')
            if method=='Size':
                body=body.replace(') Size()',') SizeIdentity()').replace('\n\tl = m.Command.Size()\n\tn += 1 + l + sovMessages(uint64(l))','')
            else:
                body=body.replace(') MarshalToSizedBuffer(',') MarshalIdentityToSizedBuffer(')
                body,count=re.subn(r'\n\t\{\n\t\tsize, err := m.Command.MarshalToSizedBuffer\(dAtA\[:i\]\).*?\n\tdAtA\[i\] = 0x12','',body,flags=re.S)
                assert count==1
            source+='\n'+body+'\n'
    path.write_text(source,encoding='utf-8')


def qualify_message(typ,pkg):
    if '.' not in typ:return pkg,typ
    parts=typ.split('.');return '/'.join(parts[:-1]),parts[-1]


def append_generated(pkg, imports, body):
    """Keep every generated declaration in the package's messages.pb.go."""
    path=OUTPUT_ROOT/pkg/'messages.pb.go'
    source=path.read_text(encoding='utf-8')
    block=re.search(r'^import \(\n(.*?)\n\)',source,re.M|re.S)
    if block is None:raise ValueError(f'{pkg}: generated import block missing')
    combined={}
    for line in block[1].splitlines()+sorted(imports):
        line=line.strip()
        if not line:continue
        parsed=re.fullmatch(r'(?:(\w+|[._])\s+)?"([^"]+)"',line)
        if parsed is None:raise ValueError(f'{pkg}: unsupported import: {line}')
        alias=parsed[1] or parsed[2].rsplit('/',1)[-1]
        combined.setdefault((alias,parsed[2]),line)
    replacement='import (\n'+'\n'.join('\t'+line for line in combined.values())+'\n)'
    source=source[:block.start()]+replacement+source[block.end():]
    if pkg in EXTENSIONS:body=re.sub(r'}\n(?=func )','}\n\n',body)
    path.write_text(source+'\n'+body+'\n',encoding='utf-8')


def rules(pkg):
    imports={'"'+PREFIX+'wireproto"'}
    code=[]
    for name,fields in SCHEMAS[pkg].items():
        rows=[];norm=[]
        for field in fields:
            n=field['name'];t=field['type'];opts=[]
            if t not in SCALAR_TYPES:
                p,child=qualify_message(t,pkg)
                alias=p.split('/')[-1]+'.' if p!=pkg else ''
                if p!=pkg:imports.add('"'+PREFIX+p+'"')
                opts.append('Message:&'+alias+'WireShape'+child)
                if field['repeated']:norm.append('for i:=range m.'+n+' { if err:=m.'+n+'[i].NormalizeProto();err!=nil{return err} }' if not field['pointer'] else 'for _,v:=range m.'+n+' {if v!=nil {if err:=v.NormalizeProto();err!=nil{return err}}}')
                elif field['pointer']:norm.append('if m.'+n+'!=nil {if err:=m.'+n+'.NormalizeProto();err!=nil{return err}}')
                elif p=='replica/defs' and child=='Propose' and pkg!='replica/defs':
                    norm.append('if err:=m.'+n+'.Command.NormalizeProto();err!=nil{return err}')
                else:norm.append('if err:=m.'+n+'.NormalizeProto();err!=nil{return err}')
            cast=field['cast']
            if cast:
                short=cast.removeprefix(PREFIX)
                base=MANIFEST['aliases'].get(short,cast)
                base=MANIFEST['aliases'].get(pkg+'.'+base,base)
                if base in ['byte','uint8','uint16']:opts.append('UnsignedBits:'+('8' if base!='uint16' else '16'))
                if base in ['int8','int16']:opts.append('SignedBits:'+base[3:])
            if field['hash']:opts.append('Width:32')
            if field['repeated'] and t in ['uint32','uint64','sint32','sint64']:opts.append('Packed:true')
            if opts:rows.append(str(field['number'])+':{'+','.join(opts)+'}')
            if field['repeated'] and (pkg in ['epaxos','swift'] or (pkg,name) in [('kcensus','Message'),('fastpaxos','Message'),('curp','MAAcks')]) and (pkg,name)!=('swift','MSyncDepsEntry'):
                norm.append('wireproto.EmptySlice(&m.'+n+')')
        if pkg=='epaxos' and any(f['name']=='Command' for f in fields):norm.append('for i:=range m.Command {wireproto.EmptySlice(&m.Command[i].V)}')
        if pkg=='swift' and name=='MNewLeaderAckN':norm.append('for i:=range m.Cmds {wireproto.EmptySlice(&m.Cmds[i].V)}')
        if pkg in ['paxos','curp'] and name=='Request':norm.append('if len(m.Command.V)==0 {m.Command.V=nil}')
        if pkg=='curp' and name=='MAccept':norm.append('wireproto.EmptySlice(&m.Cmd.V)')
        if (pkg,name) in [('curp','MReply'),('curp','MSyncReply'),('swift','MAccept'),('swift','MReply')]:norm.append('wireproto.EmptySlice(&m.Rep)')
        if pkg=='swift' and name=='MSync':norm.append('if err:=m.validateRecoveryEntries();err!=nil{return err};for i:=range m.Cmds {if len(m.Cmds[i].Value.V)==0 {m.Cmds[i].Value.V=nil}}')
        if pkg=='replica/defs' and name in ['Propose','ProposeAndRead']:norm.append('wireproto.EmptySlice(&m.Command.V)')
        if pkg=='replica/defs' and name in ['ProposeReplyTS','ReadReply','ProposeAndReadReply']:norm.append('wireproto.EmptySlice(&m.Value)')
        if pkg=='kcensus' and name=='Message':norm.append('if err:=m.normalizeAbsentValues();err!=nil{return err}')
        code.extend(['var WireShape'+name+'=wireproto.Shape{'+','.join(rows)+'}', 'func(m *'+name+') ValidateProtoPayload(data []byte, depth int)error{return WireShape'+name+'.Validate(data,depth)}', 'func(m *'+name+') NormalizeProto()error{if m==nil{return nil};'+';'.join(norm)+';return nil}'])
    return imports, '\n'.join(code)


def streams(pkg):
    roots=MANIFEST['roots'].get(pkg,[])
    if not roots:return set(), ''
    code=[]
    for r in roots:
        typ=r['type'];model=r['model'][0].upper()+r['model'][1:]
        limit=r.get('limit',{'kcensus':'maxFrame','bodega':'maxFrame','paxos':'16<<20','curp':'16<<20'}.get(pkg,'wireproto.MaxFrame'))
        obj='&m.message' if typ=='wireMessage' else 'm'
        projection=''
        if r.get('project'):
            projection=f"value:=m.{r['project']}();"
            obj='&value'
        code.extend([f'func(m *{typ}) Marshal(w io.Writer){{{projection}wireproto.Write(w,{obj},{limit})}}',f'func(m *{typ}) MarshalProtoFrame(code uint8,tagged bool)([]byte,error){{{projection}return wireproto.Encode(code,tagged,{obj},{limit})}}',f'func(m *{typ}) Unmarshal(r io.Reader)error{{var value {model};if err:=wireproto.Read(r,&value,{limit});err!=nil{{return err}}'])
        if pkg in ['kcensus','bodega','fastpaxos']:code.append('if err:=validateProtoMessage(&value);err!=nil{return err}')
        if pkg in EXTENSIONS and r.get('validate'):code.append(f"if err:={r['validate']}(&value);err!=nil{{return err}}")
        code.append('*m=wireMessage{message:value};return nil}' if typ=='wireMessage' else '*m=value;return nil}')
        if pkg in EXTENSIONS:code.append(f'func(*{typ}) New()fastrpc.Serializable{{return &{typ}{{}}}}')
        if typ in MANIFEST.get('binary_sizes',{}).get(pkg,[]):code.append(f'func(m *{typ}) BinarySize()(int,bool){{return 0,false}}')
    imports={'"io"','"'+PREFIX+'wireproto"'}
    if pkg in EXTENSIONS:imports.add('fastrpc "'+PREFIX+'rpc"')
    return imports, '\n'.join(code)


def generate(packages=None, *, source_root=None, output_root=None, plugin=None):
    """Generate selected packages; staging roots let templates publish atomically."""
    global SOURCE_ROOT, OUTPUT_ROOT, SCHEMAS, MANIFEST, EXTENSIONS
    SOURCE_ROOT=Path(source_root or ROOT).resolve()
    OUTPUT_ROOT=Path(output_root or SOURCE_ROOT).resolve()
    if not SOURCE_ROOT.is_relative_to(ROOT) or not OUTPUT_ROOT.is_relative_to(ROOT):
        raise ValueError('source/output roots must stay inside ConsensusArena')
    selected=[package_path(p,SOURCE_ROOT) for p in (PACKAGES if packages is None else packages)]
    if not selected or len(selected)!=len(set(selected)):raise ValueError('select distinct packages')
    MANIFEST=copy.deepcopy(BASE_MANIFEST)
    SCHEMAS={p:messages(p) for p in selected}
    EXTENSIONS=set(selected)-set(PACKAGES)
    for pkg in EXTENSIONS:extension_config(pkg)
    protoc=shutil.which('protoc')
    gogo=Path(plugin).resolve() if plugin else shutil.which('protoc-gen-gogo')
    if not protoc:raise ValueError('protoc not found on PATH')
    if not gogo or not Path(gogo).is_file():
        raise ValueError('protoc-gen-gogo not found; supply --plugin PATH or install cosmos/gogoproto v1.7.2 on PATH')
    dependency=json.loads(subprocess.check_output(['go','list','-m','-json','github.com/cosmos/gogoproto'],cwd=ROOT,text=True))['Dir']
    schemas=[p+'/messages.proto' for p in selected]
    command=[protoc,'-I'+str(SOURCE_ROOT),'-I'+str(ROOT),'-I'+dependency,'-I'+str(Path(dependency)/'protobuf'),
             '--plugin=protoc-gen-gogo='+str(gogo),'--gogo_out=paths=source_relative:'+str(OUTPUT_ROOT)]
    for schema in schemas:
        generated=subprocess.run(command+[schema],cwd=ROOT,capture_output=True,text=True)
        if generated.returncode:raise RuntimeError(generated.stderr)
    for pkg in selected:
        patch_generated(pkg)
        rule_imports,rule_body=rules(pkg)
        stream_imports,stream_body=streams(pkg)
        aliases=MANIFEST.get('local_names',{}).get(pkg,{})
        alias_body='\n'.join('type '+local+' = '+name for local,name in aliases.items())
        append_generated(pkg,rule_imports|stream_imports,'\n\n'.join([alias_body,rule_body,stream_body]))
    paths=[str(OUTPUT_ROOT/p/'messages.pb.go') for p in selected]
    subprocess.run(['gofmt','-w']+paths,check=True)
    result=dict(packages=len(selected),generated_messages=sum(map(len,SCHEMAS.values())),conversion_adapters=0)
    print(json.dumps(result))
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--package',action='append',dest='packages',help='Generate only this module-relative package; repeat to select several. Default: existing baseline/shared packages only.')
    parser.add_argument('--plugin',help='Path to protoc-gen-gogo v1.7.2; otherwise use PATH')
    args=parser.parse_args()
    try:generate(args.packages,plugin=args.plugin)
    except (ValueError,OSError,subprocess.CalledProcessError,RuntimeError) as exc:parser.exit(1,str(exc)+'\n')

if __name__=='__main__':main()
