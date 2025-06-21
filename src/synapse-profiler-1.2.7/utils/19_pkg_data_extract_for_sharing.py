# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Run Settings

# COMMAND ----------

# DBTITLE 0,Run Settings
# MAGIC %run ../00_settings

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. data_staging_root DBFS path check

# COMMAND ----------

# DBTITLE 0,Check "data_staging_root" DBFS path for folder/files
import os
#workspace_name = synapse_workspace["name"]
workspace_name='combined' #for the complete list of worksapces
#data_staging_root = os.path.join(synapse_profiler["data_staging_root"], workspace_name)
data_staging_root = 'dbfs:/FileStore/synapse-profiler-testing/' #original staging root directory

if not data_staging_root:
  raise ValueError("Error: Missing required 'data_staging_root' setting")

print(f"Workspace Name: {workspace_name}")
# list using dbfs
dbutils.fs.ls(data_staging_root)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. data_staging_root local copy

# COMMAND ----------

# DBTITLE 0,iii) Check "data_staging_root" LINUX path for folder/files
from datetime import datetime
import shutil, os, subprocess

# tmpFolder
xtimestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
xtmpFolder = os.path.join("/tmp", f"{workspace_name}_{xtimestamp}")
print(f"INFO: creating tmpFolder {xtmpFolder} ...")
# create tmp folder
os.mkdir(xtmpFolder)
# create workspace folder in tmp folder
data_staging_root_local = os.path.join(xtmpFolder, workspace_name)
print(f"INFO: creating data_staging_root_local {data_staging_root_local} ...")
os.mkdir(data_staging_root_local)

# COMMAND ----------

# DBTITLE 1,make local copy
data_staging_root_local_url = "file:" + data_staging_root_local
print(f"INFO: data_staging_root_local_url: {data_staging_root_local_url} ...")
result = dbutils.fs.cp(data_staging_root, data_staging_root_local_url, True)
if not result:
  raise RuntimeError(f"Error: Failed to make a local copy of data extract for packing!!! from: {data_staging_root} to: {data_staging_root_local_url}")

# COMMAND ----------

display(dbutils.fs.ls(data_staging_root_local_url))

# COMMAND ----------

# DBTITLE 1,subfolders2pkg

filePathList = [os.path.join(data_staging_root_local,entry) for entry in os.listdir(data_staging_root_local)]
subfolders2pkg =  [entry for entry in filePathList if os.path.isdir(entry)]
if not subfolders2pkg:
  raise RuntimeError("ERROR: No Profiler Data Extract folder/files found ")
else:
  print(f"INFO: Subfolders to package:")
  for idx, entry in enumerate(subfolders2pkg):
    print(f"\t{idx}  {entry}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Create single zip file to share

# COMMAND ----------

# DBTITLE 1,Create single zip files to share
from datetime import datetime
import shutil, os, subprocess

# tmpFolder
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
tmpFolder = os.path.join("/tmp", f"{workspace_name}_{timestamp}")
print(f"INFO: creating tmpFolder {tmpFolder} ...")
# create tmp folder
os.mkdir(tmpFolder)

shareArchive=f'{tmpFolder}.zip'
print(f"INFO: Creating Shared Archive '{shareArchive}' \n\twith contents from \n{os.linesep.join(subfolders2pkg)} folder...\n\n")

# Run zip with subprocess
zip_proc = subprocess.Popen(
    f'zip -r {shareArchive} {" ".join(subfolders2pkg)}', 
  shell=True, 
  stdout=subprocess.PIPE, 
  stderr=subprocess.STDOUT,
  universal_newlines=True
  )
# print stdout/stderr while it is running
while zip_proc.poll() is None:
  output_line = zip_proc.stdout.readline()
  print(output_line)

# Finally Exit status
if zip_proc.returncode != 0:
  raise RuntimeError(f"Error: Failed to run {zip_proc.args} with returncode {zip_proc.returncode}")

# COMMAND ----------

print(subprocess.check_output(f'ls -l {shareArchive}', shell=True, stderr=subprocess.STDOUT).decode())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Encrypt the single zip file to share

# COMMAND ----------

# DBTITLE 1,set public key
# we are going to use the public key encrypion using gpg
public_key_tx = """-----BEGIN PGP PUBLIC KEY BLOCK-----

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
Qzzek6FgzTw9DA==
=4T/I
-----END PGP PUBLIC KEY BLOCK-----"""
# import the public key
print(subprocess.check_output(f'echo "{public_key_tx}" | gpg --import', shell=True, stderr=subprocess.STDOUT).decode())

# COMMAND ----------

# DBTITLE 1,Encrypt the zip file created in step#5
encryptedShareArchive = f'{shareArchive}.gpg'
result = subprocess.run(
  f'gpg --batch --output {encryptedShareArchive} --encrypt --recipient migration.sa@databricks.com --always-trust {shareArchive}', 
  shell=True, 
  stdout=subprocess.PIPE, 
  stderr=subprocess.STDOUT
  )
print(result.args)
print(result.stdout.decode())
result.check_returncode()
print(subprocess.check_output(f'ls -l {encryptedShareArchive}', shell=True, stderr=subprocess.STDOUT).decode())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Place the Encrypted Zip file in data_staging_root

# COMMAND ----------

# DBTITLE 1,Move from tmp to data_staging_root
encryptedShareArchiveUrl = "file:" + encryptedShareArchive
dbutils.fs.cp(encryptedShareArchiveUrl, data_staging_root)

# COMMAND ----------

# DBTITLE 1,Print File to share info
file2Share = dbutils.fs.ls(os.path.join(data_staging_root, os.path.basename(encryptedShareArchive)))
print(file2Share)
print("******************* SHARE THE FOLLOWING FILE ********************************\n")
print(f"File Name      : {file2Share[0].name}")
print(f"DBFS File path : {file2Share[0].path}")
print(f"File Size      : {file2Share[0].size} Bytes")

# COMMAND ----------

# DBTITLE 1,Clean up local folders
print(f"Info: Removing {xtmpFolder}")
dbutils.fs.rm("file:" + xtmpFolder, True)
print(f"Info: Removing {tmpFolder}")
dbutils.fs.rm("file:" + tmpFolder, True)

# COMMAND ----------

