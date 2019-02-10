from pathlib import Path
import zipfile
import os

pth = Path(r'/Users/markberman/cs598_CCC/raw_data/airline_ontime')
dirs = [x for x in pth.iterdir() if x.is_dir()]

for d in dirs:
    zips = list(d.glob('*.zip'))
    if zips == []:
        print(str(d) + ' path is already unzipped')
        continue

    os.chdir(d)
    for z in zips:
        with zipfile.ZipFile(z) as myzip:
            for m in myzip.namelist():
                f = Path(m)
                if f.suffix == '.csv':
                    myzip.extract(f.name)
                break
        os.remove(z)

    print(str(d) + ' has been unzipped')
