##########################################
#Leitor de Noticias by Bruno Rafaeli#
##########################################
library(stringr)
library(parallel)

#Lista com os jornais
jornais = c('@JornalOGlobo','@jc_pe','@cbonlinedf','@em_com','@folha','@Estadao','@zerohora','@atarde')

#Define quais arquivos quer ler
files_0 <- (Sys.glob("../new_noticias_capturadas/new_noticias_capturada/*"))

# Calculate the number of cores
no_cores <- 7

# Initiate cluster
cl <- makeCluster(no_cores)


leitor <- function(j){

  jornal = j
  
  final_data <- data.frame(
    date = character(),
    n_encontrado = integer(),
    n_total = integer()

  )
  
  caminho_0 = paste0("/*",jornal,"*")
  
  for(i in 1:length(files_0)){
    
    caminho = paste0(files_0[i],caminho_0)
    
    files <- (Sys.glob(caminho))
    
    total = 0
    total_incerteza = 0
    
    data = unlist(strsplit(files_0[i],"/"))
    data = data[[3]]
    
    if(length(files) > 0){
      
      tempo_inicial = Sys.time()
      contador = 0
    
      for(j in 1:length(files)){
      

        contador = contador + 1
        
        total = total + 1 
        
        fileName <- files[j]
        artigo <- readChar(fileName, file.info(fileName)$size)
        artigo <- toupper(artigo)
        
        economic_words <- c("ECON")
        
        uncertainty_words <- c("INCERT","INSTAB")#,"CRISE")
        
        politic_words <- c("GOVERNO","CONGRESSO","DILMA","PRESIDENT")
        
        combina_termos = do.call(paste, expand.grid(economic_words,uncertainty_words, politic_words))
        #combina_termos = do.call(paste, expand.grid(economic_words,uncertainty_words))
        
        for(k in 1:length(combina_termos)){
          
          termo = unlist(strsplit(combina_termos[k]," "))
          
          resultado = all(str_detect(artigo,termo))
          
          if(resultado){
            
            total_incerteza = total_incerteza + 1

            break
            
          }
          
        }
        
        
        if(contador == 1000){
          
          tempo_final = Sys.time() - tempo_inicial
          message(i, " | ", tempo_final)
          contador = 0
        }
      
        
      }
      
      new_row <- data.frame(
        date = data,
        n_encontrado = total_incerteza,
        n_total = total
      )
      
      final_data <- rbind(final_data,new_row)
      
      message(paste0(files_0[i]," | ",i,"/", length(files_0)))
      
      }
    
  }
  
  nome = paste0("3_statistic_",jornal,".Rda")
  saveRDS(object = final_data,file = nome)

}


executar_leitor <- function(i){
  
  leitor(jornais[i])
  
}

clusterExport(cl=cl, varlist=c("jornais","files_0","leitor"))
clusterEvalQ(cl,library(stringr))

numero_rows = length(jornais)-1

parLapply(cl,1:numero_rows,executar_leitor)

stopCluster(cl)



