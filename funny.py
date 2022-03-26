import pandas as pd
from functools import partial, reduce
import numpy as np
import sys
import re
import utils
from functools import partial

def chunk_read(it, chunk_size):
    l = list()
    for x in it: 
        l.append(x)
        if len(l) >= chunk_size:
            yield l
            l.clear()
    yield l
def read_dataframes(pstr, chunk_size=100000):
    p = re.compile(pstr)
    def read(lines) -> pd.DataFrame:
        def readline(line):
            m = p.match(line)
            return pd.Series(dict(m.groupdict())) if m else None
        dicts = filter(lambda x: x is not None, (readline(line) for line in lines))
        for chunk in chunk_read(dicts, chunk_size):
            yield pd.concat(chunk, axis=1).T
    return read
def readLines(file:str):
    with open(file, 'r', encoding='utf8', buffering=2<<24) as f:
        # while True:
        for i in range(15123):
            line = f.readline()
            if line: yield line.strip()
            else: return 'done'
        yield 'done'
def writeLines(file:str, lines):
    with open(file, 'w', encoding='utf8', buffering=2<<24) as f:
        for line in lines:
            f.write(line + "\n")
date = '20190102_135913'
outdir = logdir = r'D:\backup\alm压测\%s\\' % date
sqllog = logdir + r'server\raw_log\sql\server-1_sql.log'
rpclog = logdir + r'server\raw_log\rpc\server-1_rpcapp.log'
redislog = logdir + r'server\raw_log\redis\server-1_redis.log'
cmdlog = logdir + r'server\raw_log\prof\server-1_prof_detail.log'

cmd_pattern = r'^(?P<logtime>\d\d\:\d\d\:\d\d)\:\d\d\d/(?P<ext>[^/]+)/(?P<cmd>[^/]+)/(?P<exectime>\d+)/(?P<waittime>\d+)/(?P<uuid>[0-9a-f\-]+)?$'
chunk_size = 100000
encoding='ANSI'

# def p99(sr:pd.Series): return sr.quantile(.99)
# def p95(sr:pd.Series): return sr.quantile(.95)
# def p75(sr:pd.Series): return sr.quantile(.75)
# def p50(sr:pd.Series): return sr.quantile(.50)

@utils.benchmark()
def stat_cost(pattern, infile, outfile, group_cols, cal_col):
    def map_(df:pd.DataFrame):
        df = df.astype({cal_col:'int'})
        gb = df.groupby(group_cols+[cal_col])[cal_col].agg('count')
        if isinstance(gb, pd.Series): gb = pd.DataFrame(gb)
        return gb
    def reduce_(df1:pd.DataFrame, df2:pd.DataFrame):
        df = df1.join(df2, how='outer', lsuffix='_x', rsuffix='_y')
        df.fillna(0, inplace=True)
        df[cal_col] = df['%s_x'%cal_col] + df['%s_y'%cal_col]
        return df.loc[:, [cal_col]]
    def stat(sr: pd.Series, group_cols, cal_col):
        sr.rename(columns={cal_col:'count'}, inplace=True)
        df = sr.reset_index()
        gb = df.groupby(group_cols).agg({'count':'sum', cal_col:['min', 'max']})
        return gb
    read = read_dataframes(pattern, chunk_size=chunk_size)
    sr = reduce(reduce_, map(map_, read(readLines(infile))))
    sr  = stat(sr, group_cols, cal_col)
    sr.to_csv(outdir + outfile, encoding=encoding)
    return sr
stat_cost(cmd_pattern, cmdlog, 'cmd_cost.csv',  ['ext', 'cmd'], 'exectime')
# statcost('redis', readRedisLog(redislog, ['exectime','uuid','cmd']))
# statcost('sql',readSqlLog(sqllog, ['exectime','uuid','cmd']))
# statcost('rpc', readRpcLog(rpclog, ['exectime','uuid','cmd']))
sys.exit()

def first(sr:pd.Series): return sr.iloc[0]
def load_cmd(): read_file_f(r'^(?P<logtime>\d\d\:\d\d\:\d\d)\:\d\d\d/(?P<ext>[^/]+)/(?P<cmd>[^/]+)/(?P<exectime>\d+)/(?P<waittime>\d+)/(?P<uuid>[0-9a-f\-]+)?$')(readLines(''))
def load_sql(): read_file_f(r'')(readLines(''))
# sqlpatterns = {'select':searchLine(r"(?i)select[ ]+.* from[ ]+`?(\w+)`?.*"),
#         'replace':searchLine(r"(?i)replace[ ]+(?:into )?`?(\w+)`?.*"),
#         'insert':searchLine(r"(?i)insert[ ]+into[ ]+`?(\w+)`?.*"),
#         'update':searchLine(r"(?i)update[ ]+`?(\w+)`?.*"),
#         'delete':searchLine(r"(?i)delete.*[ ]+from[ ]+`?(\w+)`?.*")}
# sql_p = r'^(?P<logtime>\d\d\:\d\d\:\d\d)\:\d\d\d (?P<uuid>[0-9a-f\-]+)? <db.cmd>(?P<exectime>\d+)ms: (?P<cmd>[^/]+)(?:/\[.*\]\.{0,3})?$'
def load_redis(): read_file_f(r'^(?P<logtime>\d\d\:\d\d\:\d\d)\:\d\d\d/(?P<oper>\w+)/(?P<key>\w+)/(?P<exectime>\d+)/\d+/(?P<uuid>[0-9a-f\-]+)?$')(readLines(''))
def load_rpc(): read_file_f(r'^(?P<logtime>\d\d\:\d\d\:\d\d)\:\d\d\d/(?P<oper>[^/]+)/(?P<key>[^/]+)/(?P<exectime>\d+)/\d+/(?P<uuid>[0-9a-f\-]+)?$')(readLines(''))
def stat_cost(df:pd.DataFrame, cmd_col, cal_col):
    gb = df.groupby(cmd_col).agg({cal_col:['count','min','avg','max', p50,p75,p95,99], 'uuid':first})
    return gb.droplevel(level=0, axis=1).rename(columns={cmd_col:'cmd', 'first':'uuid'})
def stat_by_min(df:pd.DataFrame, cmd_col, cal_col):
    df['minute'] = df['time'].apply(lambda x:x[:5])
    return df.groupby([cmd_col,'minute']).agg({cal_col:['count','sum']}).droplevel(level=0, axis=1)
if __name__ == '__main__':
    sys.exit()
## stat ########################################################
'''
def statUUID():
    def uf(x):return x.uuid
    cmds = filter(uf, readCmdLog(cmdlog,['ext','cmd','exectime','waittime','uuid']))
    def c0(x,y):x[0],x[1] = x[0]+1, y
    cmds = group(cmds, lambda x:x.uuid, lambda:[0,None], c0)
    def f1(o,x): o[0],o[1] = o[0] + x.exectime, o[1] + 1
    def readMap(l): return group(filter(uf, l), lambda x:x.uuid, lambda:[0,0], f1)
    sqls = readMap(readSqlLog(sqllog,['uuid','exectime']))
    rpcs = readMap(readRpcLog(rpclog,['uuid','exectime']))
    redises = readMap(readRedisLog(redislog,['uuid','exectime']))
    def f(uuid,m): return m[uuid] if uuid in m else (0,0)
    for v in cmds.values():
        cmdcount = v[0]
        cmd = v[1]
        uuid = cmd.uuid
        exectime = cmd.exectime
        sqltime, sqlcount = f(uuid, sqls)
        redistime, rediscount = f(uuid,redises)
        rpctime, rpccount = f(uuid,rpcs)
        cputime = exectime - sqltime - rpctime - redistime
        if exectime <= 12 and sqlcount <= 2 and rediscount <=2  and rpccount <= 1: continue
        yield [cmd.ext, cmd.cmd, cmdcount,sqlcount, rediscount, rpccount, uuid, exectime, cmd.waittime, sqltime, rpctime, redistime, cputime]
    return 'done'
def counttop(l, kf, num):
    def imax(o, n):
        if not o: return n
        else: return n if kf(n) > kf(o) else o
    l = list(group(l, lambda x:x[0]+'_'+x[1], None, imax).values())
    l.sort(key=kf, reverse=True)
    return l[:num]
@profilelog
def statcmd():
    titles = ['ext','cmd', 'cmdcount','sqlcount', 'rediscount', 'rpccount', 'uuid', 'exectime', 'waittime', 'sqltime', 'rpctime', 'redistime', 'cputime']
    l = list(statUUID())
    writeCsv(outdir+'cmd-sqlcount.csv', titles, counttop(l, lambda x: x[3], 100))
    writeCsv(outdir+'cmd-rediscount.csv', titles, counttop(l, lambda x: x[4], 100))
    writeCsv(outdir+'cmd-rpccount.csv', titles, counttop(l, lambda x: x[5], 100))
    writeCsv(outdir+'cmd-exectime.csv', titles, counttop(l, lambda x: x[7], 100))
@profilelog
def outputWarnUUidDetail(name, read, log, details,mincount=4, mintime=100):
    def getdata(details):
        l=read(log, ['uuid','exectime']+details)
        l=list(filter(lambda x:x.uuid, l))
        def f(o,n): o[0],o[1] = o[0]+1,o[1]+n.exectime
        uuids = group(l, lambda x:x.uuid, lambda:[0,0], f)
        uuids = map(lambda x:[x[0], x[1][0], x[1][1]], uuids.items())
        uuids = dict(map(lambda x:(x[0], x[1]), filter(lambda x:x[1]>=mincount or x[2]>=mintime, uuids)))
        l = list(filter(lambda x: x.uuid in uuids, l))
        cmds = filter(lambda x:x.uuid in uuids, readCmdLog(cmdlog, ['uuid', 'ext', 'cmd', 'logtime']))
        cmds = group(cmds, lambda x:x.uuid, None, lambda x,y:y)
        for x in l:
            if x.uuid in cmds:
                cmd = cmds[x.uuid]
                yield [cmd.logtime, cmd.ext, cmd.cmd, uuids[x.uuid], x.uuid, x.exectime] + [getattr(x,k) for k in details]
        return 'done'
    writeCsv(logdir + 'warn_%s.csv' % name, ['logtime', 'ext', 'cmd', 'count', 'uuid', 'exectime'] + details, getdata(details))
## main ####################################################################

statcmd()
statcost('redis', readRedisLog(redislog, ['exectime','uuid','cmd']))
statcost('sql',readSqlLog(sqllog, ['exectime','uuid','cmd']))
statcost('rpc', readRpcLog(rpclog, ['exectime','uuid','cmd']))
outputGroupbyMin('sql',readSqlLog(sqllog,['logtime','cmd', 'exectime']))
outputGroupbyMin('redis',readRedisLog(redislog,['logtime','cmd', 'exectime']))
outputGroupbyMin('rpc', readRpcLog(rpclog,['logtime','cmd', 'exectime']))
outputWarnUUidDetail('sql', readSqlLog, sqllog, ['sqltype', 'sqltable', 'cmd'], 4)
outputWarnUUidDetail('redis', readRedisLog, redislog, ['oper', 'key'], 8)
outputWarnUUidDetail('rpc', readRpcLog, rpclog, ['oper', 'key'], 1)
'''