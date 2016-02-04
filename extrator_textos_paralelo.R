#######################################
#Extrator de noticias by Bruno Rafaeli#
#######################################
library(rvest)
library(stringr)
library(DBI)
library(RSQLite)
library(RCurl)
library(boilerpipeR)
library(parallel)

jornais = c('@JornalOGlobo','@jc_pe','@cbonlinedf','@em_com','@folha','@Estadao','@zerohora','@atarde')

# This connection creates an empty database if it does not exist
db <- dbConnect(SQLite(), dbname = "./new_twitter_database.sqlite")

# Calculate the number of cores
no_cores <- 11

for(i in 6:length(jornais)){

# Initiate cluster
cl <- makeCluster(no_cores)

jornal = paste0("SELECT rowid,tweet_link,account,tweet_text,date from Data_Tweets WHERE account = '", jornais[i],"'", " AND ", " extraction_result = ", "'",0,"'")

assign("results", dbGetQuery(db, jornal), envir = .GlobalEnv)

message(nrow(results))

extrair_noticia <- function(x,y,z,w,v) {
  
  #db <- dbConnect(SQLite(), dbname = "./twitter_database.sqlite")
  
  url = x
  iterador = y
  conta = z
  tweet = w
  data = v
  
  #auxiliar = 0
  
  ##########Função para desencurtar links##########
  decode_short_url <- function(url, ...) {
    
    # LOCAL FUNCTIONS #
    decode <- function(u) {
      Sys.sleep(0.5)
      x <- try( getURL(u, header = TRUE, nobody = TRUE, followlocation = FALSE, cainfo = system.file("CurlSSL", "cacert.pem", package = "RCurl")) )
      #if(inherits(x, 'try-error') | length(grep(".*Location: (\\S+).*", x))<1) {
      if(inherits(x, 'try-error') | length(grep(".*[Ll]ocation: (\\S+).*", x))<1) {      
        
        return(u)
      } else {
        return(gsub('.*[Ll]ocation: (\\S+).*', '\\1', x))
      }
    }
    
    # MAIN #
    gc()
    # return decoded URLs
    urls <- c(url, ...)
    l <- vector(mode = "list", length = length(urls))
    l <- lapply(urls, decode)
    names(l) <- urls
    return(l)
  }
  
  tryCatch(
    
    {
       verificador = 0
       while(verificador < 4){
      
        url = decode_short_url(url[[1]])

        article_text = ""
        
        tryCatch(
          
          {
            conteudo_html = getURLContent(url)
          },
          
          error=function(cond) 
          {
            
            article_text <<- "Página não encontrada - Error 404"
          }
          
        )
        
        if(article_text != "Página não encontrada - Error 404")
        {
          article_text = ArticleExtractor(conteudo_html)
        }
        
        
        if(article_text == "" || toString(article_text) == "NA")
        {
          verificador = verificador + 1
        }
        
        else{
          
          verificador = 4
        }
        
      
       }
       
#        if(toString(article_text) == "NA"){
#          
#          tryCatch(
#            {
#              saida_1 = read_html(conteudo_html,encoding = "UTF-8")
#              saida_2 = html_nodes(saida_1,"p")
#              article_text = html_text(saida_2)
#              article_text = iconv(article_text,from="UTF-8", to="latin1")
#              
#              auxiliar = 2
#              
#            },
#            
#            error=function(cond) 
#            {
#              message(paste0("Tentativa de extrair o link do tweet falhou"))
#            }
#            
#          )
#        }
       
        
      if(article_text == ""){
        
        tryCatch(
          {
            tentar = strsplit(conteudo_html,"url=")
            tentar_2 = strsplit(tentar[[1]][2],'">\r' )
            tentar_3 = tentar_2[[1]][1]
            
            conteudo_html = getURLContent(tentar_3)
            
            article_text = ArticleExtractor(conteudo_html)
          },
          
          error=function(cond) 
          {
            message(paste0("Tentativa de extrair o link do tweet falhou"))
          }
          
        )
      }
       
       if(article_text == ""){
         
         tryCatch(
           {
             tentar = strsplit(conteudo_html,"url=")
             tentar_2 = strsplit(tentar[[1]][2],'">\r' )
             tentar_3 = tentar_2[[1]][1]
             
             conteudo_html = getURLContent(tentar_3)
             
             article_text = ArticleExtractor(conteudo_html)
           },
           
           error=function(cond) 
           {
             message(paste0("Tentativa de extrair o link do tweet falhou"))
           }
           
         )
       }
      
    },
    
    error=function(cond) 
    {
      message(paste0("O tweet não possui link"))
    },
    
    finally = {
      
      ifelse(!dir.exists(file.path("./new_noticias_capturadas", toString(data))), dir.create(file.path("./new_noticias_capturadas/", toString(data))), FALSE)
      
      if(article_text == "" || toString(article_text) == "NA"  ||article_text == "Página não encontrada - Error 404"){
        article_path = paste0("./new_noticias_capturadas/",toString(data),"/",iterador,"_",conta,"_texto_filtrado.txt")
        write(tweet,article_path)
        
        #query = paste0("UPDATE Data_Tweets SET article_path = ", "'" ,article_path,"'", " WHERE rowid = ", iterador )
        
        #try(dbSendQuery(conn = db, query))

        return(c(article_path,w))
      }
      else{
        
        
        article_path = paste0("./new_noticias_capturadas/",toString(data),"/",iterador,"_",conta,"_texto_filtrado.txt")
        
        
#         else{
#           article_path = paste0("./new_noticias_capturadas/",toString(data),"/",iterador,"_",conta,"_texto_filtrado_2.txt")
#           
#         }
        write(article_text,article_path)
        
        #query = paste0("UPDATE Data_Tweets SET article_path = ", "'" ,article_path,"'", "," ," extraction_result = 1 "," WHERE rowid = ", iterador )

        #try(dbSendQuery(conn = db, query))
        
        return(c(article_path,article_text))
      }
      
    }
    
  )
  
  #dbDisconnect(db)
  
}


final_noticia <- function(i){
  
  link = results$tweet_link[[i]]
  iterador = results$rowid[[i]]
  conta = results$account[[i]]
  tweet = results$tweet_text[[i]]
  data = results$date[[i]]

  caminho = paste0("./new_noticias_capturadas/",data,"/",results$rowid[[i]],"_",conta,"_texto_filtrado.txt")
  
  if(file.exists(caminho)){
   
    return(NULL)
  }
  
  extrair_noticia(link,iterador,conta,tweet,data)
  
}

numero_rows = nrow(results)

clusterExport(cl=cl, varlist=c("results","final_noticia","extrair_noticia"))

clusterEvalQ(cl,library(boilerpipeR))
clusterEvalQ(cl,library(rvest))
clusterEvalQ(cl,library(stringr))
clusterEvalQ(cl,library(RCurl))

parLapply(cl,1:numero_rows ,final_noticia)


stopCluster(cl)
}

# Close the connection to the database
dbDisconnect(db)