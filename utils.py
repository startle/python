import os
import math
import datetime
import openpyxl
import unittest
from collections import Iterable

import smtplib
from contextlib import contextmanager
from email.utils import parseaddr, formataddr
from email import encoders
from email.header import Header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart




def listFiles(dir:str):
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
def readLines(file:str):
    with open(file, 'r', encoding='utf8', buffering=2<<16) as f:
        while 1:
            line=f.readline()
            if line:
                yield line.strip()
            else:
                return 'done'
def writeLines(file:str, lines:Iterable):
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

def binsearch(l:list, e, key=None):
    if not key:
        key = lambda x:x
    n = len(l)
    bi,ei,mi = 0, n-1, 0
    if e>key(l[ei]) or e<key(l[bi]):
        return -1
    if e == l[ei]:
        return ei
    if e == l[bi]:
        return bi
    while 1:
        mi = math.floor((bi+ei)/2)
        if mi == bi or mi == ei:
            return mi
        if e == key(l[mi]):
            return mi
        elif e > key(l[mi]):
            bi = mi
        else:
            ei = mi

def writeExcel(path:str, rows:Iterable):
    wb = openpyxl.Workbook()
    wb.guess_types = True
    sheet = wb.active
    for i, row in enumerate(rows):
        if isinstance(row, (list, tuple)):
            sheet.append(row)
    wb.save(path)
def readExcel(path:str, sheet:str=None):
    wb = openpyxl.load_workbook(path)
    table = wb.active if not sheet else wb.get_sheet_by_name(sheet)
    for row in table.rows:
        yield [cell.value for cell in row]
    return row

class Email(object):
    def login(self, smtp_server, account, pwd):
        self.server = smtplib.SMTP(smtp_server, 25)
        name, self.fromaddr = parseaddr(account)
        self.server.login(self.fromaddr, pwd)
        self.account = account
    def send(self, subject:str, toaddr:list, bodys:list, cc:list=[]):
        toaddr = [_format_addr(x) for x in toaddr]
        cc = [_format_addr(x) for x in cc]
        msgroot = MIMEMultipart('related')
        msgroot['From'] = self.account
        msgroot['Subject'] = subject
        msgroot['to'] = ','.join(toaddr)
        recv = toaddr + cc
        msgroot['cc'] = ','.join(cc)
        for body in bodys:
            msgroot.attach(body)
        self.server.sendmail(self.account, toaddr, msgroot.as_string())
    def quit(self):
        self.server.quit()
def _format_addr(s):
    name, addr = parseaddr(s)
    return formataddr((Header(name, 'utf-8').encode(), addr))
@contextmanager
def loginEmail(account, pwd, smtp_server):
    email = Email()
    email.login(smtp_server, account, pwd)
    yield email
    email.quit()

class TestUntils(unittest.TestCase):
    def testExcel(self):
        file = 'D:/_python3_excel_test_.xlsx'
        if os.path.exists(file):
            os.remove(file)
        rows = [('Id', 'Name')]+[[1,'A'],[2,'B'],[3, 'C']]
        writeExcel(file, rows)
        rows = readExcel(file)
        self.assertListEqual([1,'A'], list(rows)[1])
        os.remove(file)
    def testBinSearch(self):
        l = (1,5,8,10,15,18)
        self.assertEqual(binsearch(l,1), 0)
        self.assertEqual(binsearch(l,18), 5)
        self.assertEqual(binsearch(l,19), -1)
        self.assertEqual(binsearch(l,-2), -1)
        self.assertEqual(binsearch(l,4), 0)
        self.assertEqual(binsearch(l,10), 3)
        self.assertEqual(binsearch(l,16), 4)
    def testFile(self):
        file = 'D:/_python3_file_rw_test.txt'
        if os.path.exists(file):
            os.remove(file)
        lines = ('A1','B2\t', '', 'C3 ', '\t')
        writeLines(file, lines)
        lines = list(readLines(file))
        self.assertEqual(len(lines), 5)
        self.assertEqual(lines[0], 'A1')
        self.assertEqual(lines[1], 'B2')
        self.assertEqual(lines[2], '')
        self.assertEqual(lines[3], 'C3')
        self.assertEqual(lines[4], '')
        os.remove(file)
    def testEmail(self):
        account = '大头哥哥<chenjiayao@aobi.com>'
        pwd = '????????'
        smtp_server = 'smtp.exmail.qq.com'
        
        toaddr = [account]
        cc = [account]
        msg = MIMEText('hello, send by Python...', 'plain', 'utf-8')

        with loginEmail(account, pwd, smtp_server) as email:
            email.send('test python3 email', toaddr, [msg], cc=cc)
if __name__ == '__main__':
    unittest.main()