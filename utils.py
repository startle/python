import os
import math
import datetime
import openpyxl

def listFiles(dir):
    if not os.path.exists(dir):
        return 'done'
    absdir = os.path.abspath(dir)
    list = os.listdir(absdir)
    for file in list:
        absFile = os.path.join(absdir, file)
        if os.path.isfile(absFile):
            yield absFile
        if os.path.isdir(absFile):
            for sFile in listFiles(absFile):
                yield sFile
    return 'done'
def readLines(file):
    with open(file, 'r', encoding='utf8', buffering=2<<16) as f:
        while 1:
            line=f.readline()
            if line:
                yield line
            else:
                return 'done'
def writeLines(file, lines):
    with open(file, 'w', encoding='utf8', buffering=2<<16) as f:
        for line in lines:
            f.write(line + "\n")

def benchmark(func):
    def wrapper(*args, **kw):
        b = datetime.datetime.now()
        obj = func(*args, **kw)
        e = datetime.datetime.now()
        print('(%ss)call %s:' % ((e-b).seconds, func.__name__))
        return obj
    return wrapper

def binsearch(list, e, key):
    n = len(list)
    bi,ei,mi = 0, n-1, 0
    while 1:
        mi = math.floor((bi+ei)/2)
        if mi == bi or mi == ei:
            return mi
        if e == key(list[mi]):
            return mi
        elif e > key(list[mi]):
            bi = mi
        else:
            ei = mi

def writeExcel(path, rows):
    wb = openpyxl.Workbook()
    wb.guess_types = True
    sheet = wb.active
    for i, row in enumerate(rows):
        for j, cell in enumerate(row):
            sheet.cell(row=i+1, column=j+1, value=str(cell))
    wb.save(path)
def readExcel(path, sheet=None):
    wb = openpyxl.load_workbook(path)
    table = wb.active if sheet else wb.get_sheet_by_name(sheet)
    for row in table.rows:
        yield row
    return row

def test07Excel():
    rows = [[1,2],[4,3],['A','b']]
    write07Excel('E:/临时用文件夹/uc/test.xlsx', rows)
#test07Excel()
