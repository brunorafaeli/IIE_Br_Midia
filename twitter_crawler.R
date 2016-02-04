##########################################
#WEB SCRAPING NO TWITTER by Bruno Rafaeli#
##########################################
library("RSelenium")
library(rvest)
library(stringr)
library(DBI)
library(RSQLite)
library(RCurl)


# This connection creates an empty database if it does not exist
db <- dbConnect(SQLite(), dbname = "../Desktop/indice_twitter/simula_new_twitter_database.sqlite")

tipos =list(date="date", date_ms="integer",number_of_tweets = "integer",number_of_retweets = "integer",number_of_likes = "integer",account = "character",tweet_text = "character",tweet_link = "character",article_path = "character",extraction_result = "integer")

checkForServer()
startServer()

##Iniciando uma conexão##

#Para utilizar o navegador padrão(firefox)
mybrowser <- remoteDriver()

mybrowser$open()


lista_titulos <- c('%3AJornalOGlobo','%3Ajc_pe','%3Acbonlinedf','%3Aem_com','%3Afolha','%3AEstadao','%3Azerohora','%3Aatarde','%3Ag1','%3AUOL','%3ATerra','%3AportalR7','%3Aglobocom','%3Avalor_economico', '%3ATvBandnews', '%3AGloboNews', '%3ARevistaEpoca', '%3ARevistaISTOE', '%3AUOLNoticias', '%3AVEJA', '%3Acartacapital')

lista_arquivos <- c('infla_JornalOGlobo','infla_jc_pe','infla_cbonlinedf','infla_em_com','infla_folha','infla_Estadao','infla_zerohora','infla_atarde','infla_g1','infla_UOL','infla_Terra','infla_portalR7','infla_globocom','infla_valor_economico', 'band_news', 'globo_news', 'epoca', 'istoe', 'uolnews', 'veja', 'ccapital')

lista_datas <- c('2009-08-01','2011-03-01','2009-09-01','2010-09-01','2012-03-09','2014-04-27','2015-10-19','2008-03-01','2010-01-01','2007-10-01','2009-10-01','2015-01-20','2009-09-01','2007-11-01','2009-05-01', '2009-08-11', '2010-05-10', '2009-02-18', '2009-04-08', '2008-04-29', '2008-11-28', '2008-10-07')

#lista_datas <- c('2015-04-01','2011-03-01','2009-09-01','2010-09-01','2008-05-01','2008-05-01','2007-11-01','2008-03-01','2010-01-01','2007-10-01','2009-10-01','2015-01-20','2009-09-01','2007-11-01','2009-05-01', '2009-08-11', '2010-05-10', '2009-02-18', '2009-04-08', '2008-04-29', '2008-11-28', '2008-10-07')


#lista_termos <- c('"bolo de chocolate"','"lava jato"','"petrolao"','"impeachment"','"crise politica" AND "brasileira" OR "brasileiro" OR "brasil" OR "dilma"','"lava jato" AND "petrobras"', '"lava jato" AND "sergio moro"')

lista_termos <- c('')

#lista_titulos <- c('%3AJornalOGlobo','%3Ajc_pe','%3Acbonlinedf','%3Aem_com','%3Afolha','%3AEstadao','%3Azerohora','%3Aatarde','%3Ag1','%3AUOL','%3ATerra','%3AportalR7','%3Aglobocom','%3Avalor_economico')

#lista_titulos <- c("Twitter Todo")

#lista_arquivos <- c("lava_jato_testando_o_novo")

url_pattern <- "http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"


for (word in 14:length(lista_titulos)) {

  titulo = paste0(lista_arquivos[word],".csv")
  
  #write.table("date,date_ms,number_of_tweets,number_of_retweets,number_of_likes,account,tweet_text,tweet_link,article_path,extraction_result" ,file = titulo,row.names = FALSE,col.names = FALSE)
  
  titulo_2 = paste0("tweets_",lista_arquivos[word],".csv")
  
  termo = lista_termos[1]
  
  # Decindo datas a serem pesquisadas
  data_inicio =  as.Date(lista_datas[word],format = "%Y-%m-%d")
  data_final = as.Date("2016-01-01",format = "%Y-%m-%d")  
  
  data_total <- seq(data_inicio,to = data_final, by = 'days')
  
  tamanho_data = length(data_total)
    
  #define o tamanho de cada bloco de tempo buscado ex : N = 100 = 100 dias
  N = 5
  
  if (tamanho_data > N){
    
    parte_data = round(tamanho_data/N)
    parte_final = tamanho_data - parte_data*N
    
  }
  
  else{
    
    parte_data = 1
  }
  
  pesquisa_atual = paste0("Inicializando a pesquisa na conta : ", lista_titulos[word], " | Termo = ", termo)
  message(pesquisa_atual)
  
  #Mede o tempo realizado na busca
  tempo_inicial = proc.time()
  
  total <- parte_data
  # create progress bar
  pb <- txtProgressBar(min = 0, max = total, style = 3)
  
  iterador = 0
  
  
  for (dat in 1:parte_data){
    
    Sys.sleep(1)
    
    #Cria data frame com o número de tweets x Date
    resultado_final <- data.frame(
                #date = as.Date(character()),
                date = character(),
                date_ms = integer(),
                number_of_tweets = integer(),
                number_of_retweets = integer(),
                number_of_likes = integer(),
                account = character(),
                tweet_text = character(),
                tweet_link = character(),
                article_path = character(),
                extraction_result = integer()
                )
    

    if(parte_data == 1){
      
      data_ini = data_inicio
      data_fim = data_final
    }
    
    
    else if(dat == 1){
      
      data_ini = data_total[1]
      data_fim = data_total[N]
      
    }
    
    else if(dat == parte_data ){
      data_ini = data_total[(dat-1)*N]
      #data_fim = data_total[dat*N + parte_final]
      data_fim = data_final
    }
    
    else{
      
      data_ini = data_total[(dat-1)*N]
      data_fim = data_total[(dat-1)*N + N]
      
    }
    
    ini = "https://twitter.com/search?f=tweets&vertical=default&q="
    
    fonte = paste0("from",lista_titulos[word])
    
    m_0 = "%20since%3A"
    m_1 = "%20until%3A"  
    
    #Para incluir retweets
    m_2 = "include%3Aretweets&src=typd"
    
    lingua = "%20lang%3Apt%20"
    
    #url_final = paste0(ini,termo," ",fonte)
    

    url_final = paste0(ini,termo,lingua,fonte,m_0,data_ini,m_1,data_fim,m_2)
    #url_final = paste0(ini,termo,lingua,m_0,data_ini,m_1,data_fim,m_2)
    
    variavel_auxiliar = 0
    
    while(variavel_auxiliar < 2){
    
    mybrowser$navigate(url_final)
    
    aux = 0
    
    tryCatch(
      
      {
        
        for (n in 1:1000000)
        {
          erro = 1
          testando = "ok"
          
          #Realiza scroll down na página
          mybrowser$executeScript("window.scrollTo(0, document.body.scrollHeight);")

          if(n == 1){
            
            check_1 <- mybrowser$findElement(using = "class name", "stream-end-inner")
            check_2 <- check_1$findChildElement(using = "tag name", "p")
            check_3 <- check_2$getElementText()
            
            if (check_3 == "Nenhum resultado.") {
              
              variavel_auxiliar = variavel_auxiliar + 1
              
              if(variavel_auxiliar == 2){
              mensagem_erro = paste0("Nenhum resultado entre os dias ", data_ini, " e ", data_fim)
              message(mensagem_erro)}
              Sys.sleep(2)
              break
            }
            
            else{

              variavel_auxiliar = 10
            }
          }

          check_1 <- mybrowser$findElement(using = "class name", "stream-end-inner")
          check_4 <- check_1$findChildElement(using = "tag name", "button")
          check_5 <- check_4$getElementText()
          
          
          if(n == 5){
            
            #Capturar os Tweets
            conteudo_atual <- mybrowser$getPageSource()
            conteudo_atual_1 <- read_html(conteudo_atual[[1]],encoding = "utf-8")
            conteudo_atual_2 = html_nodes(conteudo_atual_1,".tweet-text")
          
            if (length(conteudo_atual_2) < 20){
              
              break
              
            }
          }
          
          
          if (check_5 == "Voltar ao topo da página ↑" || check_5 == ""){
            
            verificador_1 <- mybrowser$findElement(using = "class name", "stream-footer")
            verificador_2 <- verificador_1$findChildElement(using = "tag name", "div")
            resultado_verificado <- verificador_2$getElementAttribute(attrName = "class")
            
            if(resultado_verificado == "timeline-end has-items"){
              
              Sys.sleep(1)
              mybrowser$executeScript("window.scrollTo(0, document.body.scrollHeight);")
              Sys.sleep(2)
              mybrowser$executeScript("window.scrollTo(0, document.body.scrollHeight);")
              
              
              verificador_1 <- mybrowser$findElement(using = "class name", "stream-footer")
              verificador_2 <- verificador_1$findChildElement(using = "tag name", "div")
              resultado_verificado <- verificador_2$getElementAttribute(attrName = "class")
              
            }
            
            if(resultado_verificado == "timeline-end has-items has-more-items"){
              
              next
              
            }
            
            
            #Realiza scroll down na página
            mybrowser$executeScript("window.scrollTo(0, document.body.scrollHeight);")
            
            mybrowser$executeScript(
              
              "if($(window).scrollTop() + screen.height > $('body').height())
              {
              alert('FIM')
              }"
          )
            
            tryCatch(
              
              {testando <- mybrowser$getAlertText()},
              
              error=function(cond) 
              {
                
                erro = 0
              },
              
              finally = 
              {
                if(testando == "FIM"){
                  mybrowser$dismissAlert()

                  break
                 
                }
                
              }     
              
            )
            
          }
                
      }
        
  },
  
  error=function(cond) {
    message("Erro na execução:")
    message(cond)
    message("Salvando os dados capturados até o momento")
  },
  
  warning = function(cond) {
    
    message("Warning na execução:")
    message(cond)
    # Choose a return value in case of warning
  },
  
  finally =
  {
    
    # update progress bar
    setTxtProgressBar(pb, dat)
    
    #Capturar os Tweets
    conteudo <- mybrowser$getPageSource()
    conteudo_1 <- read_html(conteudo[[1]],encoding = "utf-8")
    #conteudo_2 = html_nodes(conteudo_1,".tweet-text")
    conteudo_2 = html_nodes(conteudo_1,"p.TweetTextSize.js-tweet-text.tweet-text")
    

    
    conteudo_final = html_text(conteudo_2)
    
    #Encontra quantos likes o post teve
    likes = html_nodes(conteudo_1,"button.ProfileTweet-actionButton.js-actionButton.js-actionFavorite")
    
    #Encontra quantos retweets o post teve
    retweets = html_nodes(conteudo_1,"button.ProfileTweet-actionButton.js-actionButton.js-actionRetweet")

    #Encontra os nomes das contas
    accounts = html_nodes(conteudo_1,"span.username.js-action-profile-name")
    
    #tempos_0 = html_nodes(conteudo_1, ".js-short-timestamp")
    
    tempos_0 = html_nodes(conteudo_1, "span._timestamp.js-short-timestamp")
    
    numero_t = length(tempos_0)
    
    if(numero_t != length(conteudo_2)){message("Número de textos difere do número de datas")}
    
    if(numero_t >= 1){
      
      for(elem in 1:numero_t){
       
        #Time in date 
        tempos = html_attr(x = tempos_0[[elem]],name = "data-time")
        tempo_convertido = as.numeric(tempos)
        #tempo_final = as.Date(as.POSIXct(tempo_convertido, origin="1970-01-01"))
        tempo_final = as.Date(format(as.POSIXct(tempo_convertido, origin="1970-01-01"),"%Y-%m-%d"))
        #Time in miliseconds
        tempo_ms = html_attr(x = tempos_0[[elem]],name = "data-time-ms")
        
        #retweets
        children_retweet = html_children(retweets[elem])
        children_retweet = html_children(children_retweet)
        children_retweet = html_children(children_retweet)
        text_retweet = as.numeric(html_text(children_retweet))
        
        if(toString(text_retweet) == "NA"){text_retweet = 0}
        
        #likes
        children_like = html_children(likes[elem])
        children_like = html_children(children_like)
        children_like = html_children(children_like)
        text_like = as.numeric(html_text(children_like[[2]]))
        
        if(toString(text_like) == "NA"){text_like = 0}
        
        #accounts
        account_now = html_text(accounts[elem])
        
        #tweet-text
        text_of_tweet = conteudo_final[[elem]]
        
        #Remove virgula,aspas duplas e simples dos tweets
        text_of_tweet = gsub('"', '', text_of_tweet)
        text_of_tweet = gsub("'", '', text_of_tweet)
        text_of_tweet = gsub(",", '', text_of_tweet)
        
        #Link do texto
        link = str_extract(text_of_tweet, url_pattern)
        
        #Nome do arquivo
                  path_noticia = "NA"
                
                  #Texto da noticia
                  texto_noticia = "NA"
                  
                  #Extraction Result 
                  ext_r = 0
    
                            
          new_row <- data.frame(
                  date = toString(tempo_final), 
                  date_ms = as.numeric(tempo_ms),
                  number_of_tweets = 1,
                  number_of_retweets = text_retweet,
                  number_of_likes = text_like,
                  account = account_now,
                  tweet_text = text_of_tweet,
                  tweet_link = toString(link),
                  article_path = path_noticia,
                  extraction_result = ext_r 
                  )
        
        resultado_final <- rbind(resultado_final,new_row)
        
        

        
        iterador = iterador + 1
        
      }
      
      tryCatch(
        
        {

          dbWriteTable(conn = db, name = "Data_Tweets",field.types = tipos ,value = resultado_final, row.names = FALSE,append = TRUE)
          #gc(TRUE)
        },
        
        error=function(cond) {
        message(paste0("Erro ao salvar no banco de dados : ", data_inicio, "-",data_final))
          
        
        },
        
        warning = function(cond) {
          
          
        }
        
      )
    }
    
    
    
    
  }
    )
  
    }
      
  }
  
  close(pb)
  
  rm(check_1,check_2,check_3,check_4,check_5,conteudo_atual,conteudo_atual_1,conteudo_atual_2,conteudo,conteudo_1,conteudo_2,conteudo_final,tempos_0,tempos,tempo_convertido,tempo_final, new_row, resultado_final, resultado_final_2)
  
}

mybrowser$close()

# Close the connection to the database
dbDisconnect(db)