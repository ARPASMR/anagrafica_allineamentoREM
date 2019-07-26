
###############################################################################
#  4-maggio-2015   MR
#  26-luglio-2019 aggiornato e dockerizzato
#
#  esegue verifiche sulla compilazione dei campi di anagrafica nel DBmeteo
#  1. info di base per tutte le stazioni
#  2. info di dettaglio e assegnazione per le stazioni di UO
#==============================================================================
library(DBI)
library(RMySQL)
#==============================================================================
#+ gestione dell'errore
neverstop<-function(){
  print("EE..ERRORE durante l'esecuzione dello script!! Messaggio d'Errore prodotto:\n")
  quit()
}
options(show.error.messages=TRUE,error=neverstop)
#==============================================================================

dir_output<-"./"
fileout<-paste(dir_output,"verifiche.out",sep="")

#==============================================================================
#   LEGGO INFO DEL DBmeteo
#==============================================================================

drv<-dbDriver("MySQL")
conn<-try(dbConnect(drv, user="guardone", password=as.character(Sys.getenv("MYSQL_PWD")), dbname="METEO", host="10.10.0.6"))
if (inherits(conn,"try-error")) {
  print( "ERRORE nell'apertura della connessione al DBmeteo \n")
  print( " Eventuale chiusura connessione malriuscita ed uscita dal programma \n")
  dbDisconnect(conn)
  rm(conn)
  dbUnloadDriver(drv)
  quit(status=1)
}

DBmeteo<-try(dbGetQuery(conn, "
SELECT
A_Stazioni.IDstazione, NOMErete, ProprietaStazione,Localita, IDsensore, CONCAT(Comune,' ',IFNULL(Attributo,'')) as Nome, Provincia, Quota, QuotaSensore, truncate(Y(A_Sensori.CoordUTM),0) as UTM_Nord, truncate(X(A_Sensori.CoordUTM),0) as UTM_Est, truncate(Y(A_Stazioni.CoordUTM),0) as UTM_Nord_Staz, truncate(X(A_Stazioni.CoordUTM),0) as UTM_Est_Staz ,NOMEtipologia,DataInizio,DataFine,Storico, AggregazioneTemporale
FROM
A_Stazioni,A_Sensori, A_Reti
WHERE
A_Stazioni.IDstazione=A_Sensori.IDstazione and A_Stazioni.IDrete=A_Reti.IDrete"), silent=TRUE)


######################### 1 - Controlli di base di compilazione anagrafica   ######################################
cat(" ---------  DBmeteo - CONTROLLI di base di compilazione anagrafica  --------------\n", file=fileout)

cat("\n mmmmmm  Ricerca sensori in regione senza Comune o Provincia \n",file=fileout,append=T)
aux<- ((is.na(DBmeteo$Comune)==TRUE) | (is.na(DBmeteo$Provincia)==TRUE)) & DBmeteo$IDrete!=5
cat( rbind( as.vector(DBmeteo$Nome[aux]),
            as.vector(DBmeteo$IDstazione[aux]),
            as.vector(DBmeteo$Nome[aux]),
            as.vector(DBmeteo$Comune[aux]),
            as.vector(DBmeteo$Provincia[aux]),"\n" ),"\n" ,file=fileout,append=T)

cat("\n mmmmmm Ricerca stazioni senza quota\n",file=fileout,append=T)
aux<-is.na(DBmeteo$Quota)
cat( rbind( as.vector(DBmeteo$NOMErete[aux])," - ", as.vector(DBmeteo$Nome[aux]), " - ", as.vector(DBmeteo$IDsensore[aux]),as.vector(DBmeteo$NOMEtipologia[aux]),"\n" ),"\n" ,file=fileout,append=T)

cat("\n mmmmmm Ricerca sensori senza quota\n",file=fileout,append=T)
aux<-is.na(DBmeteo$QuotaSensore)
cat( rbind( as.vector(DBmeteo$NOMErete[aux])," - ", as.vector(DBmeteo$Nome[aux]), " - ", as.vector(DBmeteo$IDsensore[aux]),as.vector(DBmeteo$NOMEtipologia[aux]),"\n" ),"\n" ,file=fileout,append=T)

cat("\n mmmmmm  Ricerca stazioni senza coordinate\n",file=fileout,append=T)
aux<-is.na(DBmeteo$UTM_Nord_Staz) | is.na(DBmeteo$UTM_Est_Staz)
cat( rbind( as.vector(DBmeteo$NOMErete[aux])," - ",as.vector(DBmeteo$Nome[aux]), " - ", as.vector(DBmeteo$IDsensore[aux]),as.vector(DBmeteo$NOMEtipologia[aux]),"\n" ),"\n" ,file=fileout,append=T)

cat("\n mmmmmm  Ricerca sensori senza coordinate\n",file=fileout,append=T)
aux<-is.na(DBmeteo$UTM_Nord) | is.na(DBmeteo$UTM_Est)
cat( rbind( as.vector(DBmeteo$NOMErete[aux])," - ",as.vector(DBmeteo$Nome[aux]), " - ", as.vector(DBmeteo$IDsensore[aux]),as.vector(DBmeteo$NOMEtipologia[aux]),"\n" ),"\n" ,file=fileout,append=T)

cat("\n mmmmmm Ricerca sensori senza DataMinima\n",file=fileout,append=T)
aux<-is.na(DBmeteo$DataInizio)
cat( rbind( as.vector(DBmeteo$NOMErete[aux])," - ", as.vector(DBmeteo$Nome[aux]),  as.vector(DBmeteo$IDsensore[aux]),as.vector(DBmeteo$NOMEtipologia[aux]),"\n" ),"\n" ,file=fileout,append=T)

cat("\n mmmmmm Ricerca sensori storici senza DataMassima\n",file=fileout,append=T)
aux<-is.na(DBmeteo$DataFine) & DBmeteo$Storico=="Yes"
cat( rbind(as.vector(DBmeteo$NOMErete[aux])," - ",  as.vector(DBmeteo$Nome[aux]),  as.vector(DBmeteo$IDsensore[aux]),as.vector(DBmeteo$NOMEtipologia[aux]),"\n" ),"\n" ,file=fileout,append=T)

######################### 2 -  Controlli evoluti di compilazione anagrafica (solo per reti 1 e 4)  ###########
cat(" \n\n\n---------  DBmeteo - CONTROLLI su info di dettaglio delle stazioni INM e Aria  --------------\n", file=fileout, append=T)
DBmeteo_staz<-try(dbGetQuery(conn, "SELECT A_Stazioni.IDstazione, NOMErete, CONCAT(Comune,' ',IFNULL(Attributo,'')) as Nome, Alimentazione, Connessione, DataLogger,Fiduciaria FROM A_Stazioni, A_Reti, A_Sensori where A_Stazioni.IDrete=A_Reti.IDrete and A_Stazioni.IDstazione=A_Sensori.IDstazione and A_Stazioni.IDrete in (1,4) and Storico='No' group by A_Stazioni.IDstazione"), silent=TRUE)

cat("\n mmmmmm Ricerca stazioni attive senza info su Alimentazione\n",file=fileout,append=T)
aux<-is.na(DBmeteo_staz$Alimentazione)
cat( rbind( as.vector(DBmeteo_staz$NOMErete[aux])," - ", as.vector(DBmeteo_staz$Nome[aux]), "\n" ),"\n" ,file=fileout,append=T)

cat("\n mmmmmm Ricerca stazioni attive senza info su Connessione\n",file=fileout,append=T)
aux<-is.na(DBmeteo_staz$Connessione)
cat( rbind( as.vector(DBmeteo_staz$NOMErete[aux])," - ", as.vector(DBmeteo_staz$Nome[aux]), "\n" ),"\n" ,file=fileout,append=T)

cat("\n mmmmmm Ricerca stazioni attive senza info su Fiduciaria\n",file=fileout,append=T)
aux<-is.na(DBmeteo_staz$Fiduciaria)
cat( rbind( as.vector(DBmeteo_staz$NOMErete[aux])," - ", as.vector(DBmeteo_staz$Nome[aux]), "\n" ),"\n" ,file=fileout,append=T)

cat("\n mmmmmm Ricerca stazioni attive senza info su DataLogger\n",file=fileout,append=T)
aux<-is.na(DBmeteo_staz$DataLogger)
cat( rbind( as.vector(DBmeteo_staz$NOMErete[aux])," - ", as.vector(DBmeteo_staz$Nome[aux]), "\n" ),"\n" ,file=fileout,append=T)

cat("\n mmmmmm Ricerca sensori attivi senza info su Marca e Modello\n",file=fileout,append=T)
DBmeteo_sens<-try(dbGetQuery(conn, "
SELECT
A_Stazioni.IDstazione, NOMErete, CONCAT(Comune,' ',IFNULL(Attributo,'')) as Nome, IDsensore,NOMEtipologia
FROM
A_Stazioni,A_Sensori, A_Reti
WHERE
A_Stazioni.IDstazione=A_Sensori.IDstazione and A_Stazioni.IDrete=A_Reti.IDrete and A_Stazioni.IDrete in (1,4) and Storico='No' and A_Sensori.IDsensore not in (select IDsensore from A_Sensori_specifiche where Marca is not null and Modello is not null and (DataDisistallazione is null or DataDisistallazione ='0000-00-00'))
"), silent=TRUE)
cat( rbind( as.vector(DBmeteo_sens$NOMErete)," - ", as.vector(DBmeteo_sens$Nome),as.vector(DBmeteo_sens$IDsensore),as.vector(DBmeteo_sens$NOMEtipologia), "\n" ),"\n" ,file=fileout,append=T)

DBmeteo_pluv<-try(dbGetQuery(conn, "
SELECT
A_Stazioni.IDstazione, NOMErete, CONCAT(Comune,' ',IFNULL(Attributo,'')) as Nome, A_Sensori.IDsensore, RiscVent
FROM
A_Stazioni,A_Sensori, A_Reti, A_Sensori_specifiche
WHERE
A_Stazioni.IDstazione=A_Sensori.IDstazione and A_Stazioni.IDrete=A_Reti.IDrete and A_Sensori.IDsensore=A_Sensori_specifiche.IDsensore and NOMEtipologia='PP' and A_Stazioni.IDrete in (1,4) and Storico='No'
"), silent=TRUE)
cat("\n mmmmmm Ricerca pluviometri senza info su riscaldatore\n",file=fileout,append=T)
aux<-is.na(DBmeteo_pluv$RiscVent)
cat( rbind( as.vector(DBmeteo_pluv$NOMErete[aux])," - ",  as.vector(DBmeteo_pluv$Nome[aux]), "\n" ),"\n" ,file=fileout,append=T)

DBmeteo_ass <- try(dbGetQuery(conn, "
select  CONCAT(Comune,' ',IFNULL(Attributo,'')) as Nome , NOMErete from A_Stazioni,A_Sensori, A_Reti  where A_Stazioni.IDrete=A_Reti.IDrete and A_Stazioni.IDstazione=A_Sensori.IDstazione and A_Reti.IDrete in (1,4) and A_Stazioni.IDstazione not in (select IDstazione from StazioniAssegnate) and Storico='No' group by A_Stazioni.IDstazione
"), silent=TRUE)
print(DBmeteo_ass)
cat("\n mmmmmm Ricerca stazioni non assegnate\n",file=fileout,append=T)
cat( rbind( as.vector(DBmeteo_ass$NOMErete)," - ", as.vector(DBmeteo_ass$Nome), "\n" ),"\n" ,file=fileout,append=T)
#------------------------------------------------------------------------------
dbDisconnect(conn)
rm(conn)
#dbUnloadDriver(drv)
warnings()
quit(status=0)
