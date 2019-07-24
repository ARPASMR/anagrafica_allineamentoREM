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
   
   # verifico se è andato a buon fine
   STATO=$?
   echo "STATO USCITA SCRIPT ====> "$STATO

   if [ "$STATO" -eq 1 ] # se si sono verificate anomalie esci 
   then
       exit 1
   else # caricamento su MINIO 
       $S3CMD put allineamentoREM.out s3://rete-monitoraggio 

       # controllo sul caricamento su MINIO 
       if [ $? -ne 0 ]
       then
         echo "problema caricamento su MINIO"
         exit 1
       fi
   fi
    
   rm -f allineamentoREM.out
   
   SECONDS=0
   sleep $numsec
  fi
    
done
