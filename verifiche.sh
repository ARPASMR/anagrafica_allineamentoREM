#!/bin/bash
# 1. legge da Minio il file di anagrafica estratto dal REM ogni giorno, 
#    scrive su file eventuali discrepanze 
#    e carica il file su Minio
#
# 2.  legge da Minio il file con le estrazioni dati dal REM estratto dal REM ogni giorno, 
#     scrive su file eventuali discrepanze con quello che risulta al DBmeteo
#     e carica il file su Minio

S3CMD='s3cmd --config=config_minio.txt'
numsec=3600
SECONDS=$numsec

# leggo il file di anagrafica da Minio
     s3cmd --config=config_minio.txt --force get s3://rete-monitoraggio/AnagraficaSensori.csv ./
     

#endless loop
while [ 1 ]
do
  if [[ $(date +"%H") == "05" || ($SECONDS -ge $numsec) ]]
  then
  
   #eseguo lo script 
   Rscript A_allineamentoREM.R 
   $S3CMD put allineamentoREM.out s3://rete-monitoraggio
   rm allineamentoREM.out
   
   SECONDS=0
   sleep $numsec
  fi
    
done
