import os
import xlrd
import xlwt

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
def writeLines(file, lines):
    with open(file, 'w', encoding='utf8', buffering=2<<16) as f:
        for line in lines:
            f.write(line + "\n")
def writeExcel(file, rows):
    wb = xlwt.Workbook()
    sheet = wb.add_sheet("source")
    for i, row in enumerate(rows):
        for j, cell in enumerate(row):
            sheet.write(i, j, str(cell))
    wb.save(file)
def readExcel(file):
    data = xlrd.open_workbook(file)
    table = data.sheets()[0]
    nrows = table.nrows #行数
    for i in range(0, nrows):
        yield table.row_values(i) #某一行数据

rows = readExcel("D:/workspace/多多号.xlsx")
rows = [row for row in rows]
writeExcel("D:/workspace/out.xlsx", rows)