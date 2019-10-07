# R script
#==============================================================================
# verifiche sulle destinazioni dei sensori
# ----      ----
# 2019/03/04     GC & MR
# 2019/07/29     MR dockerizzato
#==============================================================================

library(DBI)
library(RMySQL)

# funzione per gestire eventuali errori
neverstop<-function(){
  print("EE..ERRORE durante l'esecuzione dello script!! Messaggio d'Errore prodotto:")
}
options(show.error.messages=TRUE,error=neverstop)

file_out <- "gestione_destinazioni.out"
    cat("\tcontrolli destinazioni  -  ", date(), "\n", file=file_out)
#
#___________________________________________________
#    COLLEGAMENTO AL DB
#___________________________________________________
# connessione al DB
drv<-dbDriver("MySQL")
conn<-try(dbConnect(drv, user=as.character(Sys.getenv("MYSQL_USR")), password=as.character(Sys.getenv("MYSQL_PWD")), dbname=as.character(Sys.getenv("MYSQL_DBNAME")), host=as.character(Sys.getenv("MYSQL_HOST"))))

if (inherits(conn,"try-error")) {
  print( "ERRORE nell'apertura della connessione al DB \n")
  print( "chiusura connessione malriuscita ed uscita dal programma \n")
  dbDisconnect(conn)
  rm(conn)
  dbUnloadDriver(drv)
  quit(status=1)
}


### PROTEZIONE CIVILE #########################################################################################
cat (" \n-------------------------------------\n", file=file_out, append=T)
cat (" 1. PROTEZIONE CIVILE  \n", file=file_out, append=T)
#
# tipologie di cui inviamo i dati a PC
tipologie <- "('I', 'PP','T','VV','DV','VVS','DVS','N','UR','T10','FM','FT','PO','TPP','TPN','TP','TPV','RG','RR','RN','PA')"
cat ("\tcriteri:\n\tinvio delle tipologie: ", tipologie, "\n", file=file_out, append=T)
cat ("\tdelle reti Aria e INM e dei consorzi. Aggregazione del vento scalare e vettoriale\n", file=file_out, append=T)
cat ("-------------------------------------\n", file=file_out, append=T)

####### leggo elenco delle eccezioni per questa destinazione

query <- "select IDsensore, Note from A_Sensori2Destinazione_eccezioni where Destinazione = 1 and A_Sensori2Destinazione_eccezioni.DataFine is null"
eccezioni <- try(dbGetQuery(conn, query),silent=TRUE)
if (inherits(eccezioni,"try-error")) {
  print(paste(eccezioni,"\n",sep=""))
  quit(status=1)
}

#######       CONTROLLI INVII NON DOVUTI        ###############################################################
cat(" \n\n\n\t CONTROLLO INVII NON DOVUTI:\n", file=file_out, append=TRUE)

##################          CONTROLLO CHE SIANO INVIATI SOLO SENSORI ATTIVI    ################################
 cat(" \n\t controllo che i sensori inviati a PC siano attivi\n", file=file_out, append=TRUE)
query <- " select A_Sensori.IDsensore from A_Sensori, A_Sensori2Destinazione where A_Sensori.IDsensore =A_Sensori2Destinazione.IDsensore and Storico ='yes' and Destinazione = 1 and A_Sensori2Destinazione.DataFine is null"
res <- try(dbGetQuery(conn, query),silent=TRUE)
if (inherits(res,"try-error")) {
  print(paste(res,"\n",sep=""))
  quit(status=1)
}
if(length(res$IDsensore)>0){
 i<-1
 while (i<length(res$IDsensore)+1){
 cat("ATTENZIONE!!  sensore ", res$IDsensore[i],"\t storico ma viene inviato a Protezione Civile\n", file=file_out, append=TRUE)
 i <- i + 1
 }
}else{
 cat(" \t OK , a Protezione Civile stiamo inviando solo sensori attivi\n", file=file_out, append=TRUE)
}
#
##################          CONTROLLO RETE      ################################
 cat(" \n\t controllo rete dei sensori \n", file=file_out, append=TRUE)
#
query <-"select ProprietaStazione,A_Sensori.IDsensore,IDrete,NOMEtipologia, CONCAT(Comune,' ',IFNULL(Attributo,'')) as Nome from A_Sensori, A_Sensori2Destinazione , A_Stazioni where A_Stazioni.IDstazione=A_Sensori.IDstazione and A_Sensori.IDsensore =A_Sensori2Destinazione.IDsensore and IDrete not in (1,4) and (IDrete=6 and ProprietaStazione not in ('Consorzio Adda','Consorzio Chiese','Consorzio Oglio','Consorzio Ticino'))  and Destinazione = 1 and A_Sensori2Destinazione.DataFine is null;"
#
res <- try(dbGetQuery(conn, query),silent=TRUE)
if (inherits(res,"try-error")) {
  print(paste(res,"\n",sep=""))
  quit(status=1)
}
if(length(res$IDsensore)>0){
 i<-1
 while (i<length(res$IDsensore)+1){
  if( is.element(res$IDsensore[i],eccezioni$IDsensore)==FALSE ){
  cat("ATTENZIONE!!  il sensore ", res$IDsensore[i],
     "\t "                       , res$NOMEtipologia[i],
     "\t della stazione "        ,  res$Nome[i],
     "\t della rete "            , res$IDrete[i],
     "\t inviata a PC\n"         , file=file_out, append=TRUE)
  }else{
  cat(" \tOK, trovato il sensore", res$IDsensore[i],
     "\t "                       , res$NOMEtipologia[i],
     "\t della stazione "        ,  res$Nome[i],
     "\t della rete "            , res$IDrete[i],
     "\t ma C( nella tabella delle eccezioni con nota", eccezioni$Note[res$IDsensore[i]==eccezioni$IDsensore],
     "\n"                        , file=file_out, append=TRUE)
  }
 i <- i + 1
 }
}else{
 cat(" \t OK , mandiamo solo sensori delle reti dovute\n", file=file_out, append=TRUE)
}
##################          CONTROLLO TIPOLOGIE      ################################

cat(" \n\t controllo tipologia dei sensori:\n", file=file_out, append=TRUE)
query <- paste("select A_Sensori.IDsensore,IDrete,NOMEtipologia, CONCAT(Comune,' ',IFNULL(Attributo,'')) as Nome  from A_Sensori, A_Sensori2Destinazione , A_Stazioni where A_Stazioni.IDstazione=A_Sensori.IDstazione and A_Sensori.IDsensore =A_Sensori2Destinazione.IDsensore and NOMEtipologia not in ", tipologie, " and Destinazione = 1 and A_Sensori2Destinazione.DataFine is null",sep="")
res <- try(dbGetQuery(conn, query),silent=TRUE)
if (inherits(res,"try-error")) {
  print(paste(res,"\n",sep=""))
  quit(status=1)
}
if(length(res$IDsensore)>0){
 i<-1
 while (i<length(res$IDsensore)+1){
  if( is.element(res$IDsensore[i],eccezioni$IDsensore)==FALSE ){
 cat("ATTENZIONE!!  il sensore ", res$IDsensore[i],
     "\t della stazione "        , res$Nome[i]     ,
     "\t della rete "            , res$IDrete[i],
     "\t della tipologia "       , res$NOMEtipologia[i],
     "\t viene inviato a PC\n"   , file=file_out, append=TRUE)
 }
 i <- i + 1
 }
}else{
 cat(" \t OK , mandiamo solo sensori delle tipologie concordate\n", file=file_out, append=TRUE)
}

###################################### controllo aggregazione  vento#################
cat(" \n \t Per tipologie vento controllo aggregazione:\n", file=file_out, append=TRUE)
query <- "select A_Sensori.IDsensore,IDrete,NOMEtipologia,Aggregazione, CONCAT(Comune,' ',IFNULL(Attributo,'')) as Nome  from A_Sensori, A_Sensori2Destinazione , A_Stazioni where A_Stazioni.IDstazione=A_Sensori.IDstazione and A_Sensori.IDsensore =A_Sensori2Destinazione.IDsensore and NOMEtipologia in ('VV','DV') and Aggregazione='P' and Destinazione = 1 and A_Sensori2Destinazione.DataFine is null"
res <- try(dbGetQuery(conn, query),silent=TRUE)
if (inherits(res,"try-error")) {
  print(paste(res,"\n",sep=""))
  quit(status=1)
}
if(length(res$IDsensore)>0){
 i<-1
 while (i<length(res$IDsensore)+1){
  if( is.element(res$IDsensore[i], eccezioni)==FALSE ){
 cat("ATTENZIONE!!  il sensore ", res$IDsensore[i],
     "\t della stazione "        , res$Nome[i],
     "\t della rete "            , res$IDrete[i],
     "\t della tipologia "       , res$NOMEtipologia[i],
     "\t, con aggregazione "     , res$Aggregazione[i],
     "\t viene inviato a PC\n"   , file=file_out, append=TRUE)
 }
 i <- i + 1
 }
}else{
 cat(" \tOK , mandiamo solo sensori delle aggregazioni concordate\n", file=file_out, append=TRUE)
}


##################          CONTROLLO DI COMPLETEZZA      ################################
cat(" \n\n\n \tCONTROLLO INVII MANCANTI:\n", file=file_out, append=TRUE)

cat(" \n\t cerco sensori da inviare ma NON inviati:\n", file=file_out, append=TRUE)
#
query <- paste("select A_Stazioni.IDstazione, A_Sensori.IDsensore, CONCAT(Comune,' ',IFNULL(Attributo,'')) as Nome ,CONCAT(NOMEtipologia,' ', IFNULL(Aggregazione,'')) as Tipologia  from A_Stazioni, A_Sensori where A_Stazioni.IDstazione=A_Sensori.IDstazione and A_Sensori.DataInizio is not NULL and NOMEtipologia in ", tipologie, " and (Aggregazione is NULL OR Aggregazione in ('V','S'))  and Storico='No' and (IDrete in (1,4) or (IDrete=6 and ProprietaStazione in ('Consorzio Adda','Consorzio Chiese','Consorzio Oglio','Consorzio Ticino'))) and IDsensore not in (select A_Sensori.IDsensore from A_Sensori, A_Sensori2Destinazione where A_Sensori.IDsensore =A_Sensori2Destinazione.IDsensore and Destinazione=1 and A_Sensori2Destinazione.DataFine is null) order by Provincia, Comune;", sep="")
res <- try(dbGetQuery(conn, query),silent=TRUE)
if (inherits(res,"try-error")) {
  print(paste(res,"\n",sep=""))
  quit(status=1)
}
if(length(res$IDsensore)>0){    # se c'C( almeno un sensore da inviare non inviato
 i<-1 # allora ciclo sui sensori trovati e li scrivo su file di output se non sono eccezioni
 while (i<length(res$IDsensore)+1){
  if( is.element(res$IDsensore[i],eccezioni)==FALSE ){
  cat("ATTENZIONE!!  il sensore ", res$IDsensore[i],
     "\t della tipologia "       , res$Tipologia[i],
     "\t della stazione "        , res$Nome[i],
     "\t NON inviato a PC \n"    , file=file_out, append=TRUE)
  }
  i <- i + 1
  }
 }else{
  cat("\t OK , mandiamo tutti i sensori che dobbiamo mandare\n", file=file_out, append=TRUE)
 }
#
#___________________________________________________
#    DISCONNESSIONE DAL DB
#___________________________________________________

# chiudo db
dbDisconnect(conn)
rm(conn)
dbUnloadDriver(drv)

cat( paste("\tfine controllo  ", date()," \n" ), file=file_out, append=T )
quit()
