import os

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