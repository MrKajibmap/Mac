#! /bin/sh
# Variables
# Please verify all of these on target deployments
export viya_config=/opt/sas/viya/config
export pg_bin=/opt/sas/viya/home/postgresql11/bin
export backup_dir=/data/A_LOBOK/
export pg_dbmsowner="dbmsowner"
export cps_pg_dbmsowner="dbmsowner"
export pg_port=5432
export cps_pg_port=5442
export pg_host=localhost
export cps_pg_host=localhost
export chunksize=1024
# End edit section

#Version
version_date="feb.18.2020"
echo "[Retail-Backup]: Version date: "$version_date 

#Options (to be added...)

# Backup databases
  echo "[Retail-Backup]: Backing up Retail data..."
  #export token_file=/opt/sas/viya/config/etc/SASSecurityCertificateFramework/tokens/consul/default/client.token
  source /opt/sas/viya/config/consul.conf # Introduced in Viya3.4 (18w30) 
  export CONSUL_HTTP_TOKEN=$(sudo cat /opt/sas/viya/config/etc/SASSecurityCertificateFramework/tokens/consul/default/client.token)
  export MB=1000
  #
  export PGPASSWORD="$(/opt/sas/viya/home/bin/sas-bootstrap-config kv read config/application/sas/database/postgres/password)"

  echo "[Retail-Backup]: Backing up postgres -> files"
  $pg_bin/pg_dump -h $pg_host -p $pg_port -U $pg_dbmsowner -f $backup_dir/files-backup.sql -Fc --blobs -n files SharedServices
  files_size_mb=`du -m "$backup_dir/files-backup.sql" | cut -f1`
  echo "[Retail-Backup]: Compressed size of files schema: "$files_size_mb "MB"

  echo "[Retail-Backup]: Backing up postgres -> folders"  
  $pg_bin/pg_dump -h $pg_host -p $pg_port -U $pg_dbmsowner -f $backup_dir/folders-backup.sql -Fc --blobs -n folders SharedServices
  folders_size_mb=`du -m "$backup_dir/folders-backup.sql" | cut -f1`
  echo "[Retail-Backup]: Compressed size of folders schema: "$folders_size_mb "MB"

  export PGPASSWORD="$(/opt/sas/viya/home/bin/sas-bootstrap-config kv read config/application/sas/database/cpspostgres/password)"

  echo "[Retail-Backup]: Backing up cpspostgres -> planning"
  $pg_bin/pg_dump -h $cps_pg_host -p $cps_pg_port -U $cps_pg_dbmsowner -f $backup_dir/planning-backup.sql -Fc -n planning SharedServices
  #| pv | split -d -b $chunksize - $backup_dir/planning-backup.sql".DATA.dump"
  planning_size_mb=`du -m "$backup_dir/planning-backup.sql" | cut -f1`
  echo "[Retail-Backup]: Compressed size of planning schema: "$planning_size_mb "MB"

  echo "[Retail-Backup]: Backing up cpspostgres -> processflow"
  $pg_bin/pg_dump -h $cps_pg_host -p $cps_pg_port -U $cps_pg_dbmsowner -f $backup_dir/processflow-backup.sql -Fc -n processflow SharedServices
  processflow_size_mb=`du -m "$backup_dir/processflow-backup.sql" | cut -f1`
  echo "[Retail-Backup]: Compressed size of processflow schema: "$processflow_size_mb "MB"
  
  echo "[Retail-Backup]: Backing up cpspostgres -> planningprocess"
  $pg_bin/pg_dump -h $cps_pg_host -p $cps_pg_port -U $cps_pg_dbmsowner -f $backup_dir/planning-process-backup.sql -Fc -n planningprocess SharedServices
  planningprocess_size_mb=`du -m "$backup_dir/planning-process-backup.sql" | cut -f1`
  echo "[Retail-Backup]: Compressed size of planningprocess schema: "$planningprocess_size_mb "MB"

  echo "[Retail-Backup]: Backing up cpspostgres -> demandplanning"
  $pg_bin/pg_dump -h $cps_pg_host -p $cps_pg_port -U $cps_pg_dbmsowner -f $backup_dir/demand-planning-backup.sql -Fc -n demandplanning SharedServices
  demandplanning_size_mb=`du -m "$backup_dir/demand-planning-backup.sql" | cut -f1`
  echo "[Retail-Backup]: Compressed size of demandplanning schema: "$demandplanning_size_mb "MB"


  #echo "Retail-Backup]: Compress cpsetl/stagecps"
# tar zcvf cpsetl.tar.gz /opt/sas/viya/config/data/cpsetl


echo "[Retail-Backup]: Cleanup tmp"


echo "[Retail-Backup]: Complete"
