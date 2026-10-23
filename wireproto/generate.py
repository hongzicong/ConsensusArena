"""Regenerate native protocol message types and stream glue from package schemas.

Requires protoc, Python 3 and protoc-gen-gogo from cosmos/gogoproto v1.7.2.
No protocol-to-protobuf conversion types or tests are generated.
"""
import json
from pathlib import Path
import re
import subprocess

ROOT=Path(__file__).resolve().parents[1]
HERE=ROOT/'wireproto'
PREFIX='github.com/hongzicong/ConsensusArena/'
PACKAGES=['state','replica/defs','paxos','curp','epaxos','swift','fastpaxos','kcensus','bodega']
MANIFEST=json.loads((HERE/'manifest.json').read_text(encoding='utf-8'))
LOCAL_FIELDS={('kcensus','Message'):'References bool // Local encoding policy; never transmitted.',('epaxos','PrepareReply'):'VBallot int32 // Local field, intentionally absent from the wire.'}


def messages(pkg):
    source=(ROOT/pkg/'messages.proto').read_text(encoding='utf-8')
    out={}
    for match in re.finditer(r'message (\w+) \{(.*?)\n\}',source,re.S):
        fields=[]
        for f in re.finditer(r'(optional|repeated) ([\w.]+) (\w+) = (\d+)([^;]*);',match[2]):
            custom=re.search(r'customname\)\s*=\s*"([^"]+)"',f[5])
            cast=re.search(r'casttype\)\s*=\s*"([^"]+)"',f[5])
            fields.append(dict(name=custom[1] if custom else ''.join(s.title() for s in f[3].split('_')),number=int(f[4]),type=f[2],repeated=f[1]=='repeated',pointer=f[2][0].isupper() and 'nullable) = false' not in f[5],cast=cast[1] if cast else None,hash='wiretypes.Hash' in f[5]))
        out[match[1]]=fields
    return out

SCHEMAS={p:messages(p) for p in PACKAGES}


def generated_functions(source, method):
    for m in re.finditer(r'func \(m \*(\w+)\) '+method+r'\(',source):
        start=source.index('{',m.start());depth=1;end=start+1
        while depth:
            if source[end]=='{':depth+=1
            elif source[end]=='}':depth-=1
            end+=1
        yield m[1],m.start(),end,source[m.start():end]


def patch_generated(pkg):
    path=ROOT/pkg/'messages.pb.go'
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
    path=ROOT/pkg/'messages.pb.go'
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
    path.write_text(source+'\n'+body+'\n',encoding='utf-8')


def rules(pkg):
    imports={'"'+PREFIX+'wireproto"'}
    code=[]
    for name,fields in SCHEMAS[pkg].items():
        rows=[];norm=[]
        for field in fields:
            n=field['name'];t=field['type'];opts=[]
            if t not in ['bool','string','bytes','uint32','uint64','sint32','sint64']:
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
        limit={'kcensus':'maxFrame','bodega':'maxFrame','paxos':'16<<20','curp':'16<<20'}.get(pkg,'wireproto.MaxFrame')
        obj='&m.message' if typ=='wireMessage' else 'm'
        code.extend([f'func(m *{typ}) Marshal(w io.Writer){{wireproto.Write(w,{obj},{limit})}}',f'func(m *{typ}) MarshalProtoFrame(code uint8,tagged bool)([]byte,error){{return wireproto.Encode(code,tagged,{obj},{limit})}}',f'func(m *{typ}) Unmarshal(r io.Reader)error{{var value {model};if err:=wireproto.Read(r,&value,{limit});err!=nil{{return err}}'])
        if pkg in ['kcensus','bodega','fastpaxos']:code.append('if err:=validateProtoMessage(&value);err!=nil{return err}')
        code.append('*m=wireMessage{message:value};return nil}' if typ=='wireMessage' else '*m=value;return nil}')
        if typ in MANIFEST.get('binary_sizes',{}).get(pkg,[]):code.append(f'func(m *{typ}) BinarySize()(int,bool){{return 0,false}}')
    return {'"io"','"'+PREFIX+'wireproto"'}, '\n'.join(code)


def main():
    dependency=json.loads(subprocess.check_output(['go','list','-m','-json','github.com/cosmos/gogoproto'],cwd=ROOT,text=True))['Dir']
    schemas=[p+'/messages.proto' for p in PACKAGES]
    command=['protoc','-I.', '-I'+dependency,'-I'+str(Path(dependency)/'protobuf'),'--gogo_out=paths=source_relative:.']
    for schema in schemas:
        generated=subprocess.run(command+[schema],cwd=ROOT,capture_output=True,text=True)
        if generated.returncode:raise RuntimeError(generated.stderr)
    for pkg in PACKAGES:
        patch_generated(pkg)
        rule_imports,rule_body=rules(pkg)
        stream_imports,stream_body=streams(pkg)
        aliases=MANIFEST.get('local_names',{}).get(pkg,{})
        alias_body='\n'.join('type '+local+' = '+name for local,name in aliases.items())
        append_generated(pkg,rule_imports|stream_imports,'\n\n'.join([alias_body,rule_body,stream_body]))
    paths=[str(ROOT/p/'messages.pb.go') for p in PACKAGES]
    subprocess.run(['gofmt','-w']+paths,check=True)
    print(json.dumps(dict(packages=len(PACKAGES),generated_messages=sum(map(len,SCHEMAS.values())),conversion_adapters=0)))

if __name__=='__main__':main()
