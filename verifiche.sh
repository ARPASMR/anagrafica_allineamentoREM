#!/bin/bash
# 1. legge da Minio il file di anagrafica estratto dal REM ogni giorno, 
#    scrive su file eventuali discrepanze 
#    e carica il file su Minio
#
# 2.  legge da Minio il file con le estrazioni dati dal REM estratto dal REM ogni giorno, 
#     scrive su file eventuali discrepanze con quello che risulta al DBmeteo
#     e carica il file su Minio
#
# 3.  esegue verifiche sulla compilazione dei campi di anagrafica nel DBmeteo
#     scrive su file eventuali lacune
#     e carica il file su Minio
#
###############  non eseguito perchè sostituito da dashboard di grafana #########
# 4.  esegue verifiche sui sensori inviati a PC
#     scrive su file eventuali discrepanze rispetto agli invii attesi
#     e carica il file su Minio
#################################################################################

S3CMD='s3cmd --config=config_minio.txt'
numsec=3600
SECONDS=$numsec

#endless loop
while [ 1 ]
do
  if [[ $(date +"%H") == "05" || ($SECONDS -ge $numsec) ]]
  then
  
  
  ################# 1 #################################
   # leggo il file di anagrafica da Minio
     s3cmd --config=config_minio.txt --force get s3://rete-monitoraggio/AnagraficaSensori.csv ./
     
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
       $S3CMD put PC_e_FormWeb.out s3://rete-monitoraggio 

       # controllo sul caricamento su MINIO 
       if [ $? -ne 0 ]
       then
         echo "problema caricamento su MINIO"
         exit 1
       fi
   fi
    
   rm -f allineamentoREM.out
   
    
   ################# 2 #################################
   
   # leggo il file di anagrafica da Minio
     s3cmd --config=config_minio.txt --force get s3://rete-monitoraggio/AnagraficaEstrazioni.csv ./
     
   #eseguo lo script 
   Rscript estrazioni.R 
   
   # verifico se è andato a buon fine
   STATO=$?
   echo "STATO USCITA SCRIPT ====> "$STATO

   if [ "$STATO" -eq 1 ] # se si sono verificate anomalie esci 
   then
       exit 1
   else # caricamento su MINIO 
       $S3CMD put diff_estrazioni.out s3://rete-monitoraggio 
       
       # controllo sul caricamento su MINIO 
       if [ $? -ne 0 ]
       then
         echo "problema caricamento su MINIO"
         exit 1
       fi
   fi
    
   rm -f diff_estrazioni.out
   
   ################# 3 #################################
   
   Rscript A_verifiche.R 
   
   # verifico se è andato a buon fine
   STATO=$?
   echo "STATO USCITA SCRIPT ====> "$STATO

   if [ "$STATO" -eq 1 ] # se si sono verificate anomalie esci 
   then
       exit 1
   else # caricamento su MINIO 
       $S3CMD put verifiche.out s3://rete-monitoraggio 
       
       # controllo sul caricamento su MINIO 
       if [ $? -ne 0 ]
       then
         echo "problema caricamento su MINIO"
         exit 1
       fi
   fi
    
   rm -f verifiche.out
   
   ################# 3 #################################
   
  # Rscript gestione_destinazioni.R 
  # 
  # verifico se è andato a buon fine
  #STATO=$?
  #echo "STATO USCITA SCRIPT ====> "$STATO

   #if [ "$STATO" -eq 1 ] # se si sono verificate anomalie esci 
   #then
   #    exit 1
   #else # caricamento su MINIO 
   #    $S3CMD put gestione_destinazioni.out s3://rete-monitoraggio 
   #    
   #    # controllo sul caricamento su MINIO 
   #    if [ $? -ne 0 ]
   #    then
   #      echo "problema caricamento su MINIO"
   #      exit 1
   #    fi
   #fi
    
  #  rm -f gestione_destinazioni.out
   
   SECONDS=0
   sleep $numsec
  fi
    
done
