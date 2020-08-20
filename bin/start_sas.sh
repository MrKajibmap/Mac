#! /bin/sh
etl_data_root=/data
etl_config_root=/opt/sas/mcd_config/config

# prepare logging
logroot=$etl_data_root/logs
logdir=$logroot/$(date +%Y-%m)
logdir=$logdir/$(date +%Y-%m-%d)
logdir=$logdir/$2

mkdir -p $logdir

if test -z "$3"
then
	logname=$2
else
	logname=$3
fi
logname=${logname}_$(date +%H%M%S).log

tmpfile=$(mktemp /tmp/meu_autoexec.XXXXXX)
cat /opt/sas/viya/config/etc/batchserver/default/autoexec.sas >> $tmpfile
cat $etl_config_root/autoexec.sas >> $tmpfile

sas_options="-log \"$logdir/$logname\" -autoexec \"$tmpfile\""

# run SAS
/opt/sas/viya/config/etc/batchserver/default/batchserver.sh -sysin $1 $sas_options

sas_rc=$?

#If warning, exit with 0
if [ $sas_rc -eq 0 -o $sas_rc -eq 1 ]
then
  exit 0
else
  exit $sas_rc
fi
