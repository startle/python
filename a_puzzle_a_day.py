from threading import local
import numpy as np
import datetime
import sys

__zhous = ['周%s'%s for s in '一二三四五六日']
__yues = ['%s月'%s for s in list('一二三四五六七八九十') + ['十一','十二']]
init_board = ['一月','二月','三月','四月','五月','六月', None,'七月','八月','九月','十月','十一月','十二月', None] \
            + list(range(1, 32))\
            + ['周日','周一','周二','周三',None,None,None,None,'周四','周五','周六']
init_board = np.reshape(init_board, [8, 7])
empty_board = init_board.copy()
empty_board[empty_board != None] = 0
empty_board[empty_board == None] = 99
puzzles = [
    '1111|1',
    '111|1|1',
    '111|0011',
    '111|101',
    '111|01|01',
    '11|01|011',
    '111|11',
    '11|011',
    '1111',
    '111|1'
]
def to_array(s):
    ls = s.split('|')
    height = len(ls)
    width = max([len(l) for l in ls])
    ps = np.ones([height, width])
    ps[:] = 0
    for i, l in enumerate(ls):
        l = [int(i) for i in l]
        ps[i, 0:len(l)] = l
    return ps
def array_shift(p:np.ndarray, trangle, t):
    if t : p = p.T
    if trangle == 0: pass
    elif trangle == 180: 
        n_p = p.reshape(p.size)[::-1]
        p = n_p.reshape(p.shape)
    elif trangle == 90:
        p = p.T[::-1]
    elif trangle == 270:
        p = p.T[::-1]
        n_p = p.reshape(p.size)[::-1]
        p = n_p.reshape(p.shape)
    return p
puzzle_caches={}
def p_shifts(p:str):
    if p in puzzle_caches:
        return puzzle_caches[p]
    else:
        pset = {}
        for t in [False, True]:
            for trangle in [0, 90, 180, 270]:
                n_p = array_shift(to_array(p[1]), trangle, t) * p[0]
                key = str(n_p)
                pset[key] = n_p
        puzzle_caches[p] = list(pset.values())
        return puzzle_caches[p]
puzzles = [(id+1, p) for id, p in enumerate(puzzles)]
def recursion(board, puzzles:set, answers:list):
    bh,bw = board.shape
    def next():
        for i in range(bh):
            for j in range(bw):
                if board[i, j] == 0: return i, j
        return -1, -1
    i, j = next()
    if i == -1: return
    py = j
    for pstr in puzzles.copy():
        for p in p_shifts(pstr):
            ph, pw = p.shape
            p_mask:np.ndarray = p > 0
            px = i - (p[0].argmax())
            if px<0 or px+ph>bh or j+pw>bw: continue
            b_sub:np.ndarray = board[px:px+ph, py:py+pw]
            if not (p_mask & (b_sub > 0)).any():
                b_sub += p
                puzzles.remove(pstr)
                if len(puzzles) == 0:
                    answers.append(np.array(board).astype(int))
                else:
                    recursion(board, puzzles, answers)
                b_sub[p_mask] = 0
                puzzles.add(pstr)

def mark(board, find_board:np.ndarray, keys):
    for key in keys:
        ma = find_board==key
        x = max(np.argmax(ma, axis=0))
        y = max(np.argmax(ma, axis=1))
        if find_board[x,y] != key: raise KeyError('not found.[%s]'%key)
        board[x,y] = 77

if __name__ == '__main__':
    board = empty_board.copy()
    now = datetime.datetime.now()
    # now = datetime.datetime.strptime('20220326', '%Y%m%d')
    month = __yues[now.month-1]
    week = __zhous[now.weekday()]
    day = now.day
    print('match:%s %s %s' % (month, week, day))
    mark(board, init_board, [month, day, week])
    answers = []
    recursion(board, set(puzzles), answers)
    for answer in answers:
        print('-----------------')
        print(answer)
    print('answer count:', len(answer))


