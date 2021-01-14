from couchbase.cluster import Cluster, ClusterOptions, QueryOptions
from couchbase_core.cluster import PasswordAuthenticator
import couchbase.subdocument as SD
from couchbase.exceptions import DocumentNotFoundException

from datetime import datetime
from os import listdir
from os.path import isfile, join
import os.path
import csv
import time
import re
import shutil
import random
import string

from tqdm import trange, tqdm


def split_file(file_path):
    file_name = os.path.basename(file_path)
    directory_name = os.path.dirname(file_path)
    script_path = os.path.dirname(os.path.realpath(__file__))

    num_rows = len(open(file_path).readlines())

    if num_rows >= 100000:
        print(f"Splitting file: {file_name}")
        os.system(f"cd {directory_name} && split -l 100000 -d {file_name} {file_name} && cd {script_path}")

        os.remove(file_path)

        with open(f"{file_path}00", "r") as f:
            header = f.readline()

        for fl in os.listdir(directory_name):
            if fl[-2:] != "00":
                with open(directory_name + "/" + fl, "r") as f:
                    content = f.read()

                with open(directory_name + "/" + fl, "w") as f:
                    f.write(header + content)



def extract_balance_sheet_type(filename):
    bil = filename
    
    if "." in bil:
        bil = bil.split('.')[0]
    
    bil = bil.replace("web", "")
    
    if bil[0] == "_":
        bil = bil[1:]
        
    substr = re.search(r"20[0-9]{2}", bil).group(0)
    bil = bil.replace(substr, "")

    if bil[-1:] == "_":
        bil = bil[:-1]
        
    if bil[-2:] == "an":
        bil = bil[:-2]
        
    if bil[-1:] == "_":
        bil = bil[:-1]
    
    return bil


def copy_files(src, dest, ext):
    src_files = os.listdir(src)
    print(len(src_files), src_files)
    for file_name in src_files:
        curr_ext = os.path.splitext(file_name)[1]
        if curr_ext == ext:
            full_file_name = os.path.join(src, file_name)
            shutil.copy(full_file_name, dest)


print("Creating the Couchbase cluster connection string")
cluster = Cluster(
    'couchbase://159.65.126.255', 
    ClusterOptions(PasswordAuthenticator('listafirme', 'admin.123'))
)
bucket = cluster.bucket('firme')


print("Creating temporary data directories")
path_company_id = "data/date-identificare"
path_company_fin = "data/financiare"

suffix = ''.join(random.choices(string.ascii_uppercase + string.digits + string.ascii_lowercase, k=20))
tmp_dir_data = "temp_data_" + suffix
tmp_dir_data_id = tmp_dir_data + "/date-identificare"
tmp_dir_data_fin = tmp_dir_data + "/financiare"

os.mkdir(tmp_dir_data)
os.mkdir(tmp_dir_data_id)
os.mkdir(tmp_dir_data_fin)

print("Copying original files to the temporary data directories")
copy_files(path_company_id, tmp_dir_data_id, ".csv")
copy_files(path_company_fin, tmp_dir_data_fin, ".txt")

print("Splitting input identification data files into 100.000 row chunks")
for fl in listdir(tmp_dir_data_id):
    split_file(tmp_dir_data_id + "/" + fl)

print("Splitting input financial data files into 100.000 row chunks")
for fl in listdir(tmp_dir_data_fin):
    split_file(tmp_dir_data_fin + "/" + fl)


id_src_files = [f for f in listdir(tmp_dir_data_id)]
n=0
for fl in id_src_files:
    with open(tmp_dir_data_id + "/" + fl, "r") as f:
        cnt = len(f.readlines())
        n+=cnt
print(n)


#begin = datetime.now()
print("Importing company identification data")
pbar = tqdm(total=len(id_src_files))
print(len(id_src_files), id_src_files)

for src_file in id_src_files:
    print(f'Importing the file {src_file}')
    with open(path_company_id + '/' + src_file, 'r') as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            active = 0
            
            if row[18] in ["INREGISTRAT", "TRANSFER(SOSIRE)", "TRANSFER(PLECARE)", "RELUARE ACTIVITATE", "MODIFICARE PUBLICI"]:
                active = 1
                
            if active == 1:
                nr_ordine = row[10] + "/" + row[11] + "/" + row[12]
                if nr_ordine[:2] == "//":
                    nr_ordine = ""

                db_record = {
                    "type":"firme",
                    "activa":active,
                    "cod_fiscal":row[0],
                    "nume":row[1],
                    "adresa":{
                        "loc":row[2],
                        "str":row[3],
                        "nr":row[4],
                        "detalii_adresa":row[39],
                        "cp":row[16],
                        "judet":row[19],
                        "sect":row[8],
                    },
                    "nr_ordine_recom":{
                        "nr_ordine":nr_ordine,
                        "jud_com":row[10],
                        "nr_com":row[11],
                        "an_com":row[12],                    
                    },
                    "stare":row[18],
                    "data_stare":row[17],
                    "tva":row[14],
                    "imp_micro":row[20],
                    "imp_profit":row[21]
                    
                    #"impozit":{
                    #    "imp_100":row[20],
                    #    "imp_120":row[21],
                    #    "imp_200":row[22],
                    #    "imp_410":row[23],
                    #    "imp_416":row[24],
                    #    "imp_420":row[25],
                    #    "imp_423":row[26],
                    #    "imp_430":row[27],
                    #    "imp_439":row[28],
                    #    "imp_500":row[29],
                    #    "imp_602":row[30],
                    #    "imp_701":row[31],
                    #    "imp_710":row[32],
                    #    "imp_755":row[33],
                    #    "imp_756":row[34],
                    #    "imp_412":row[36],
                    #    "imp_480":row[37],
                    #    "imp_432":row[38],
                    #},
                    #"date_contact":{
                    #    "fax":row[7],
                    #    "tel":row[9]
                    #},
                    #"sfarsit":row[15],
                    #"act_aut":row[13],
                    #"di":row[5],
                    #"dp":row[6],
                    #"bilanturi":row[35]  
                }
                
                db_record_key = f'firme::{row[0]}'
                qry = bucket.upsert(db_record_key, db_record)
    
    pbar.update(1)
    #print('Pausing for 60 seconds for database buffer cleaning')
    time.sleep(1)
    
pbar.close()
#elapsed = int((datetime.now() - x).total_seconds())
#print(f"Time elapsed: {elapsed // 60 } minutes {elapsed % 60 } seconds")


fin_src_files = [f for f in listdir(path_company_fin) if isfile(join(path_company_fin, f))]
#begin = datetime.now()
print("Importing balance sheet data data")
pbar = tqdm(total=len(fin_src_files))

n=0
for src_file in fin_src_files:
    n+=1
    #print(f'Importing the file {src_file}')
    with open(path_company_fin + '/' + src_file, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)[1:]
        year = re.search(r"20[0-9]{2}", src_file).group(0)

        for row in reader:
            cui = row[0]
            
            try:
                db_key=f'firme::{cui}'
                #print(n,src_file, db_key)
                query = bucket.get(db_key)
                activa = query.value["activa"]
                if activa == 1:
                    bilant = row[1:]
                    for i in range(1, len(bilant)):
                        if bilant[i] == '':
                            bilant[i] = 0
                        bilant[i] = int(bilant[i])                

                    obj = {}
                    for key in header:
                        for value in bilant:
                            obj[key] = value
                            bilant.remove(value)
                            break

                    db_path = f"financiare:{year}"
                    bucket.mutate_in(db_key, 
                                     [SD.upsert(
                                         db_path,
                                         obj, 
                                         create_parents=True
                                         )
                                     ]
                                    )

                    db_path = "tip_bilant"
                    bucket.mutate_in(db_key, 
                                     [SD.upsert(
                                         db_path,
                                         extract_balance_sheet_type(src_file), 
                                         create_parents=True
                                         )
                                     ]
                                    )
                
            except DocumentNotFoundException:
                pass
                    
    #time.sleep(60)
    pbar.update(1)


pbar.close()
#elapsed = int((datetime.now() - x).total_seconds())
#print(f"Time elapsed: {elapsed // 60 } minutes {elapsed % 60 } seconds")


print("Import process finished successfully.")

