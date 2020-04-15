import paramiko
from io import StringIO
from stat import S_ISDIR
import os
import PyPDF2
import shutil
import time
import logging
import glob
import yaml


## LOAD CONFIG FILE ##
with open(r"C:\Users\sgudapati3\config.yaml", 'r') as stream:
    config = yaml.load(stream,Loader=yaml.FullLoader)

## Load Parameters
log_file = config['log_file']
ssh_config = config['ssh_config']
windows_data_config = config['windows_data_config']
linux_data_conifg = config['linux_data_config']

## Load log file
logging.basicConfig(filename=log_file, format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S',level=logging.INFO)
logging.info('Initiating..')

## load private file -- created using putty key gen and converted to openssh format.
k = paramiko.RSAKey.from_private_key_file(ssh_config['key'])

## connect to paramiko ssh client
c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect( hostname = ssh_config['hostname'], username = ssh_config['username'], pkey = k )

logging.info("Connected to SSH Client")

## CHECK FOR PDF FILES TO PROCESS
source  = windows_data_config['pdf_dir']
target = linux_data_conifg['pdf_dir']
pdf_files_count = len(glob.glob(os.path.join(source,"*.pdf")))
logging.info("Found {} pdfs".format(pdf_files_count))


## COPY PDF FILES FROM SOURCE(IN WINDOWS) TO TARGET(IN LINUX)
ftp_client=c.open_sftp()
for item in os.listdir(source):
    if item.endswith(".pdf"): ## make sure only pdf files are moved.
        try:
            with open(os.path.join(source,item), "rb") as f:
                PyPDF2.PdfFileReader(f) ## check if pdf is corrupted or not.
            ftp_client.put(os.path.join(source,item),'%s/%s' % (target, item)) ## move pdf since its valid one
            shutil.move(os.path.join(source,item), windows_data_config['processed_pdf_dir']) ## move processed pdfs.
            logging.info("Processed pdf {}".format(item))
        except:
            ## corrupted pdfs..move to corrupted_pdf_cml folder.
            shutil.move(os.path.join(source,item), windows_data_config['corrupted_pdf_dir'])
            logging.warning("Corrupted pdf: {} found, moving to corrupted folder".format(item))

## SLEEP FOR ~7 MINUTES TO MAKE SURE PDF2FIGURES SCRIPT RUNS IN LINUX $$$
logging.info("Waiting for results from linux.....")
if pdf_files_count:
    time.sleep(400) 

## PULL THE REQUIRED FILES BACK TO WINDOWS FROM PDFFIGURES2 IN LINUX
def sftp_walk(sftp,remotepath):
    path=remotepath
    files=[]
    folders=[]
    for f in sftp.listdir_attr(remotepath):
        if S_ISDIR(f.st_mode):
            folders.append(f.filename)
        else:
            files.append(f.filename)
    if files:
        yield path, files
    for folder in folders:
        new_path=os.path.join(remotepath,folder)
        for x in sftp_walk(new_path):
            yield x

def clean_files(path,ends_with):
    filesInRemoteArtifacts = ftp_client.listdir(path)
    for file in filesInRemoteArtifacts:
        if file.endswith(ends_with):
            ftp_client.remove(path+file)


figures_count = len(ftp_client.listdir(path=linux_data_conifg['figures_dir']))
logging.info("Found {} figures and transfering back to windows..".format(figures_count))
for path,files  in sftp_walk(ftp_client,linux_data_conifg['figures_dir']):
    for file in files:
        ftp_client.get(os.path.join(os.path.join(path,file)), os.path.join(windows_data_config['figures_dir'],file))
    
logging.info("Transfering aggregated data file..")
ftp_client.get(os.path.join(linux_data_conifg['agg_data_dir'],'aggregated_figure_data.csv'),
                    os.path.join(windows_data_config['agg_data_dir'],'aggregated_figure_data.csv'))

logging.info("Cleaning pdfs folder")
## REMOVE PDFS FILES IN PDFFIGURES2 LIBRARY.
clean_files(linux_data_conifg['pdf_dir'],"pdf")

logging.info("Cleaning figures folder")
## REMOVE FIGURES FILES IN PDFFIGURES2 LIBRARY.
clean_files(linux_data_conifg['figures_dir'],"png")

logging.info("Cleaning data folder")
## REMOVE DATA FILES IN PDFFIGURES2 LIBRARY.
clean_files(linux_data_conifg['data_dir'],"json")

logging.info("Closing connection.")
ftp_client.close()