remove(list=ls())
setwd("C:/Users/pedro/OneDrive/01 - Backups/Área de Trabalho/R/Projeto_Final_MESP")
# Conectar ao banco de dados SQLite (cria um novo se não existir)
library(RSQLite)
library(writexl)
library(readxl)
#install.packages("quarto")
#install.packages("rmarkdown")

# Caminho para a pasta no Google Drive
db_path <- "mesp_previdencia.db"

conn <- dbConnect(RSQLite::SQLite(), db_path)

# Lista de scripts .sql que você deseja executar
scripts <- c("01-prev-processo-sqlite3.sql", "02-prev-vinculo-sqlite3.sql", "03-prev-fundamento-sqlite3.sql", "04-prev-sqlite3.sql", "05-prev-arquivo-sqlite3.sql", "06-prev-periodo-sqlite3.sql", "07-prev-proventos-sqlite3.sql")

# Loop para executar cada script
for (script in scripts) {
  # Ler o conteúdo do script SQL
  sql_script <- readLines(script, warn = FALSE)
  
  tryCatch({
    # Executar o script SQL
    dbExecute(conn, sql_script)
    print(paste("Script", script, "executado com sucesso!"))
  }, error = function(e) {
    print(paste("Erro durante a execução do script", script, ":"))
    print(e)
  })
}

# Commit para salvar as alterações no banco de dados
dbCommit(conn)

dbDisconnect(conn)

# Comando SQL para criar tabela auxiliar no banco de dados consolidando os relatórios por processo
sql_command <- "
CREATE TABLE qtd_rels_por_processo AS
SELECT processo_fk, COUNT(*) AS total_registros
FROM prev_arquivo
WHERE tipo = 'Relatório da Auditoria'
GROUP BY processo_fk
ORDER BY total_registros DESC
"

# Conectar ao banco de dados SQLite
conn <- dbConnect(SQLite(), db_path)

# Exemplo: Executar uma consulta SQL
rows <- dbGetQuery(conn, sql_command)

# Commit para salvar as alterações no banco de dados
dbCommit(conn)

# Fechar a conexão
dbDisconnect(conn)

# Comando SQL para cruzar as tabelas do banco de dados e gerar uma primeira versão da tabel consolidada de processos
library(DBI)
library(dplyr)

# Conectar ao banco de dados SQLite
conn <- dbConnect(RSQLite::SQLite(), db_path)

# Exemplo: Executar uma consulta SQL
sql_command <- "
CREATE TABLE tabela_consolidada AS
SELECT
  main_tbl.id,
  main_tbl.tipo,
  main_tbl.datalaudomedico,
  main_tbl.revisao,
  main_tbl.datainicioincapacidade,
  aux_proc.protocolo,
  aux_proc.jurisdicionado,
  aux_proc.exercicio,
  aux_vinculo.orgao,
  aux_vinculo.carreira,
  aux_vinculo.cargo,
  aux_vinculo.lotacao,
  aux_vinculo.dataingressocarreira,
  aux_vinculo.datapossecargo,
  aux_vinculo.datapublicacaoconcessaobeneficio,
  aux_vinculo.aposentadoriaespecial,
  aux_vinculo.meiopublicacaoconcessaobeneficio,
  aux_vinculo.datanomeacao,
  aux_fundamento.legislacao,
  aux_fundamento.descricao,
  aux_fundamento.tipoato,
  aux_proc.total_registros as qtd_relatorios_auditoria
FROM (SELECT * FROM prev WHERE LOWER(tipo) LIKE '%aposentadoria%') AS main_tbl
LEFT JOIN (SELECT * FROM prev_processo LEFT JOIN qtd_rels_por_processo ON prev_processo.id = qtd_rels_por_processo.processo_fk) as aux_proc ON main_tbl.processo_fk = aux_proc.id
LEFT JOIN (SELECT * FROM prev_vinculo) as aux_vinculo ON main_tbl.vinculo_fk = aux_vinculo.id
LEFT JOIN (SELECT * FROM prev_fundamento) as aux_fundamento ON main_tbl.fundamentolegal_fk = aux_fundamento.id
WHERE LOWER(aux_proc.subcategoria) LIKE '%aposentadoria%' AND aux_fundamento.antesec103 = 1
"

# Executar a consulta SQL
result <- dbGetQuery(conn, sql_command)

# Exibir os resultados
print(result)

# Commit para salvar as alterações no banco de dados
dbCommit(conn)

# Trazer tabela consolidada e tabela prev_periodo para dataframes
consulta_1 <- "SELECT * from tabela_consolidada;"
consulta_2 <- "SELECT * FROM prev_periodo WHERE prev_fk IN (SELECT id FROM tabela_consolidada);"
consulta_3 <- "SELECT * FROM prev_proventos WHERE prev_fk IN (SELECT id FROM tabela_consolidada);"

# Execute SQL query and store in dataframe
df_principal <- dbGetQuery(conn, consulta_1)
df_prevperiodo <- dbGetQuery(conn, consulta_2)
df_prevproventos <- dbGetQuery(conn, consulta_3)

# Close database connection
dbDisconnect(conn)

# Conectar ao banco de dados SQLite
conn <- dbConnect(SQLite(), db_path)

# Converter as colunas de data para objeto Date no dataframe
df_prevperiodo$datainicial <- as.Date(df_prevperiodo$datainicial)
df_prevperiodo$datafinal <- as.Date(df_prevperiodo$datafinal)

# Calcular número de dias de cada vínculo
df_prevperiodo$dias_vinculo <- as.numeric(df_prevperiodo$datafinal - df_prevperiodo$datainicial)

# Reordenando as colunas do dataframe
df_prevperiodo <- df_prevperiodo %>% select(id, prev_fk, datainicial, datafinal, dias_vinculo, mesmacarreira, mesmocargo, servicopublico, empresa, diastempoficto, diastempomagisterio)

print(nrow(df_prevperiodo))

# Transformando o dataframe para só ter um registro para cada processo, incorporando registros de prev_periodo

# Substituindo valores nulos por uma string vazia na coluna 'empresa'
df_prevperiodo$empresa[is.na(df_prevperiodo$empresa)] <- ''

# Realizando a transformação
df_resultante <- df_prevperiodo %>%
  group_by(prev_fk) %>%
  summarise(
    soma_dias_vinculo = sum(dias_vinculo, na.rm = TRUE),
    qtd_vinculos = n(),
    qtd_vinculos_pub = sum(servicopublico, na.rm = TRUE),
    qtd_vinculos_priv = n() - sum(servicopublico, na.rm = TRUE),
    desc_empresas = paste(empresa, collapse = '/'),
    soma_dias_ficto = sum(diastempoficto, na.rm = TRUE),
    soma_dias_magisterio = sum(diastempomagisterio, na.rm = TRUE),
    qtd_vinculos_mesmo_cargo = sum(mesmocargo, na.rm = TRUE),
    qtd_vinculos_mesma_carreira = sum(mesmacarreira, na.rm = TRUE)
  ) %>%
  ungroup()

# Agregando a tabela de períodos à base principal
#df_consolidado <- merge(df_principal, df_resultante, by.x = 'id', by.y = 'prev_fk', all = FALSE)
df_consolidado <- df_principal %>%
  inner_join(df_resultante, by = c("id" = "prev_fk")) %>%
  mutate(prev_fk = id)

# Exibindo o resultado
print(colnames(df_consolidado))

# Quantidade de registros do cruzamento
print(nrow(df_consolidado))
print(ncol(df_consolidado))

library(tidyr)

# Criando uma tabela dinâmica usando pivot_wider
df_proventos_ajustada <- df_prevproventos %>%
  group_by(prev_fk, tipo) %>%
  summarise(valor = sum(valor), nome = paste(nome, collapse = " / ")) %>%
  pivot_wider(names_from = tipo, values_from = c(valor, nome))

# Resetando o índice para transformar 'prev_fk' em coluna
df_proventos_ajustada <- df_proventos_ajustada %>%
  ungroup() %>%
  as.data.frame()

# Renomeando as colunas conforme necessário
colnames(df_proventos_ajustada) <- sapply(colnames(df_proventos_ajustada), function(col) {
  if (grepl("_", col)) {
    return(col)
  } else {
    return(paste0(col, "_"))
  }
})

# Ajustada a tabela de proventos, agora estamos prontos para adicioná-la à tabela consolidada
df_consolidado_2 <- merge(df_consolidado, df_proventos_ajustada, by.x = 'id', by.y = 'prev_fk', all = FALSE)

head(df_consolidado, 20)

# Substitua 'qtd_relatorios' pelo nome real da sua coluna
df_consolidado_2 <- df_consolidado_2 %>%
  mutate(rotulo = ifelse(qtd_relatorios_auditoria == 1, 0, 1))

# Vamos retirar algumas colunas agora que não importam para o código

# Colunas para remover
colunas_para_remover <- c('prev_fk', 'nome_Média', 'nome_Remuneração', 'valor_Remuneração')

# Remove as colunas do DataFrame
df_consolidado_2 <- df_consolidado_2 %>%
  select(-all_of(colunas_para_remover))

# Exportar para um arquivo Excel
write_xlsx(df_consolidado_2, path = "Base_Conferencia_4_R.xlsx")


# Conectar ao banco de dados SQLite
conn <- dbConnect(SQLite(), db_path)

# Use o método dbWriteTable para escrever o DataFrame no banco de dados
# Substitua 'tabela_final' pelo nome que deseja para a tabela
dbWriteTable(conexao, 'tabela_final', df_consolidado_2, row.names = TRUE, overwrite = TRUE)

# Script de consulta da tabela consolidada com armazenamento em dataframe do R

# Conectar ao banco de dados SQLite
conn <- dbConnect(SQLite(), db_path)

# Exemplo de consulta SELECT
consulta_4 <- "SELECT * from tabela_final;"

# Executar a consulta e armazenar o resultado em um dataframe
df_tbl_final <- dbGetQuery(conn, consulta_4)

# Fechar a conexão
dbDisconnect(conn)

library(ggplot2)
library(dplyr)

# Excluir linhas de aposentadoria compulsória e por invalidez, conforme análise feita no excel
valor_excluir_1 <- "Aposentadoria Compulsória"
valor_excluir_2 <- "Aposentadoria por Invalidez"

df_tbl_final <- df_tbl_final %>% filter(tipo != valor_excluir_1 & tipo != valor_excluir_2)


# Criar de_para da coluna "jurisdicionado", conforme análise de composição feita no excel
# Supondo que você tenha uma lista chamada 'lista'
lista <- c('Paraíba Previdência', 'Instituto de Previdência do Município de João Pessoa', 
           'Instituto de Prev. dos Serv. Mun. de Campina Grande', 
           'Instituto de Prev. e Assistência dos Serv. Pub. do Mun. de Bayeux', 
           'Instituto de Previdência do Município de Santa Rita')

# Função para aplicar à coluna 'jurisdicionado'
verificar_jurisdicionado <- function(valor) {
  if (valor %in% lista) {
    return(valor)
  } else {
    return('Outros')
  }
}

# Aplicando a função à coluna 'jurisdicionado' para criar a nova coluna
df_tbl_final$PARA_Jurisdicionado <- sapply(df_tbl_final$jurisdicionado, verificar_jurisdicionado)

library(readxl)

# -----------------------------------------------------------------

# Contagem das ocorrências de cada conteúdo na coluna "descricao"
contagem_descricao <- table(df_tbl_final$descricao)

# Lista dos valores que ocorrem mais de 500 vezes
valores_mais_de_500 <- names(contagem_descricao[contagem_descricao > 500])

valores_mais_de_500

# Mapeando os valores para "Outros" se ocorrerem menos de 500 vezes
df_tbl_final <- df_tbl_final %>%
  mutate(PARA_descricao = ifelse(descricao %in% valores_mais_de_500, descricao, "Outros"))

# Criando flag de aposentadoria especial
df_tbl_final$PARA_aposentadoriaespecial <- ifelse(!is.na(df_tbl_final$aposentadoriaespecial), 1, 0)

# --------------------------------------------------------------------
# Importando tabela "DE_PARA_ORGAO" para cruzamento

df_orgao <- read_excel('DE_PARA_ORGAO.xlsx')

df_orgao <- df_orgao %>%
  rename(orgao = DE)

# Convertendo a coluna 'orgao' para caixa alta em df_orgao
df_orgao <- df_orgao %>%
  mutate(orgao_UC = toupper(orgao))

# Convertendo a coluna 'orgao' para caixa alta em df_tbl_final
df_tbl_final <- df_tbl_final %>%
  mutate(orgao_UC = toupper(orgao))

# Realizando a junção (merge) dos DataFrames
df_tbl_final <- df_tbl_final %>%
  left_join(df_orgao %>% select(orgao_UC, PARA_ORGAO_PADR), by = "orgao_UC")

# Contagem, cálculo do percentual e percentual acumulado
df_composicao <- df_tbl_final %>%
  count(PARA_ORGAO_PADR, name = "n") %>%
  mutate(percent = (n / sum(n)) * 100) %>%
  arrange(desc(n)) %>%
  mutate(cum_percent = cumsum(percent))

# Agrupando órgãos que têm percentuais acumulados acima de 80%
df_composicao <- df_composicao %>%
  mutate(novo_orgao = ifelse(cum_percent > 80, "OUTROS", PARA_ORGAO_PADR))

df_relacao <- df_composicao %>%
  select(PARA_ORGAO_PADR, novo_orgao) %>%
  distinct()

df_composicao <- df_composicao %>%
  group_by(novo_orgao) %>%
  summarise(n = sum(n)) %>%
  ungroup() %>%
  mutate(percent = (n / sum(n)) * 100) %>%
  arrange(desc(n)) %>%
  mutate(cum_percent = cumsum(percent))

# Criar um mapeamento de 'PARA_ORGAO_PADR' original para o novo valor (com "OUTROS")
mapping <- df_relacao %>% 
  select(PARA_ORGAO_PADR, novo_orgao)

# Substituir os valores na coluna original de df_tbl_final com base no mapeamento
df_tbl_final <- df_tbl_final %>%
  left_join(mapping, by = "PARA_ORGAO_PADR") %>%
  mutate(PARA_ORGAO_PADR = coalesce(novo_orgao, PARA_ORGAO_PADR)) %>%
  select(-novo_orgao)

# Testando que a alteração deu certo
df_composicao_2 <- df_tbl_final %>%
  count(PARA_ORGAO_PADR, name = "n") %>%
  mutate(percent = (n / sum(n)) * 100) %>%
  arrange(desc(n)) %>%
  mutate(cum_percent = cumsum(percent))

# --------------------------------------------------------------------------------

# Importando tabela "DE_PARA_CARREIRA" para cruzamento
df_carreira <- read_excel('DE_PARA_CARREIRA.xlsx')

df_carreira <- df_carreira %>%
  rename(carreira = DE)

# Convertendo a coluna 'carreira' para caixa alta em df_carreira
df_carreira <- df_carreira %>%
  mutate(carreira_UC = toupper(carreira))

# Convertendo a coluna 'carreira' para caixa alta em df_tbl_final
df_tbl_final <- df_tbl_final %>%
  mutate(carreira_UC = toupper(carreira))

# Realizando a junção (merge) dos DataFrames
df_tbl_final <- df_tbl_final %>%
  left_join(df_carreira %>% select(carreira_UC, PARA_CARREIRA_PADR), by = "carreira_UC")

# Contagem, cálculo do percentual e percentual acumulado
df_composicao_3 <- df_tbl_final %>%
  count(PARA_CARREIRA_PADR, name = "n") %>%
  mutate(percent = (n / sum(n)) * 100) %>%
  arrange(desc(n)) %>%
  mutate(cum_percent = cumsum(percent))

# Agrupando carreiras que têm percentuais acumulados acima de 90%
df_composicao_3 <- df_composicao_3 %>%
  mutate(novo_carreira = ifelse(cum_percent > 94, "OUTROS", PARA_CARREIRA_PADR))

df_relacao_carreira <- df_composicao_3 %>%
  select(PARA_CARREIRA_PADR, novo_carreira) %>%
  distinct()

df_composicao_3 <- df_composicao_3 %>%
  group_by(novo_carreira) %>%
  summarise(n = sum(n)) %>%
  ungroup() %>%
  mutate(percent = (n / sum(n)) * 100) %>%
  arrange(desc(n)) %>%
  mutate(cum_percent = cumsum(percent))

# Criar um mapeamento de 'PARA_CARREIRA_PADR' original para o novo valor (com "OUTROS")
mapping <- df_relacao_carreira

# Substituir os valores na coluna original de df_tbl_final com base no mapeamento
df_tbl_final <- df_tbl_final %>%
  left_join(mapping, by = "PARA_CARREIRA_PADR") %>%
  mutate(PARA_CARREIRA_PADR = coalesce(novo_carreira, PARA_CARREIRA_PADR)) %>%
  select(-novo_carreira)

# Testando que a alteração deu certo
df_composicao_4 <- df_tbl_final %>%
  count(PARA_CARREIRA_PADR, name = "n") %>%
  mutate(percent = (n / sum(n)) * 100) %>%
  arrange(desc(n)) %>%
  mutate(cum_percent = cumsum(percent))

# Importando tabela "DE_PARA_MEIOP" para cruzamento
df_meiop <- read_excel('DE_PARA_MEIOP.xlsx')

df_meiop <- df_meiop %>%
  rename(meiopublicacaoconcessaobeneficio = DE)

# Convertendo a coluna 'meiopublicacaoconcessaobeneficio' para caixa alta
df_meiop <- df_meiop %>%
  mutate(meiop_UC = toupper(meiopublicacaoconcessaobeneficio))

df_tbl_final <- df_tbl_final %>%
  mutate(meiop_UC = toupper(meiopublicacaoconcessaobeneficio))

# Realizando a junção (merge) dos DataFrames
df_tbl_final <- df_tbl_final %>%
  left_join(df_meiop %>% select(meiop_UC, PARA_MEIOP_PADR), by = "meiop_UC")

df_tbl_final <- df_tbl_final %>%
  mutate(PARA_MEIOP_PADR = ifelse(is.na(PARA_MEIOP_PADR), 'OUTROS', PARA_MEIOP_PADR))

# Contagem, cálculo do percentual e percentual acumulado
df_composicao_5 <- df_tbl_final %>%
  count(PARA_MEIOP_PADR, name = "n") %>%
  mutate(percent = (n / sum(n)) * 100) %>%
  arrange(desc(n)) %>%
  mutate(cum_percent = cumsum(percent))

# Agrupando órgãos que têm percentuais acumulados acima de 90%
df_composicao_5 <- df_composicao_5 %>%
  mutate(novo_meiop = ifelse(cum_percent > 90, "OUTROS", PARA_MEIOP_PADR))

df_relacao_meiop <- df_composicao_5 %>%
  select(PARA_MEIOP_PADR, novo_meiop) %>%
  distinct()

df_composicao_5 <- df_composicao_5 %>%
  group_by(novo_meiop) %>%
  summarise(n = sum(n)) %>%
  ungroup() %>%
  mutate(percent = (n / sum(n)) * 100) %>%
  arrange(desc(n)) %>%
  mutate(cum_percent = cumsum(percent))

# Criar um mapeamento de 'PARA_MEIOP_PADR' original para o novo valor (com "OUTROS")
mapping <- df_relacao_meiop %>% 
  select(PARA_MEIOP_PADR, novo_meiop)

# Substituir os valores na coluna original de df_tbl_final com base no mapeamento
df_tbl_final <- df_tbl_final %>%
  left_join(mapping, by = "PARA_MEIOP_PADR") %>%
  mutate(PARA_MEIOP_PADR = coalesce(novo_meiop, PARA_MEIOP_PADR)) %>%
  select(-novo_meiop)

# Testando que a alteração deu certo
df_composicao_6 <- df_tbl_final %>%
  count(PARA_MEIOP_PADR, name = "n") %>%
  mutate(percent = (n / sum(n)) * 100) %>%
  arrange(desc(n)) %>%
  mutate(cum_percent = cumsum(percent))

# Criar categorias para a quantidade de vínculos totais 
# Função para categorização
categorizar_vinculos <- function(qtd) {
  if (qtd == 1) {
    return('1 vínculo')
  } else if (qtd == 2) {
    return('2 vínculos')
  } else if (qtd == 3) {
    return('3 vínculos')
  } else if (qtd == 4) {
    return('4 vínculos')
  } else {
    return('5 ou mais vínculos')
  }
}

# Aplicar a função à coluna e atribuir o resultado a uma nova coluna
df_tbl_final <- df_tbl_final %>%
  mutate(qtd_vinculos_padr = sapply(qtd_vinculos, categorizar_vinculos))

# Criar categorias para a quantidade de vínculos privados 
# Função para aplicar a lógica desejada
categorizar_vinculos_priv <- function(qtd) {
  if (qtd == 0) {
    return('Nenhum vínculo privado')
  } else if (qtd == 1) {
    return('1 vínculo privado')
  } else if (qtd == 2) {
    return('2 vínculos privados')
  } else {
    return('3 ou mais vínculos privados')
  }
}

# Aplicar a função à coluna e atribuir o resultado a uma nova coluna
df_tbl_final <- df_tbl_final %>%
  mutate(qtd_vinculos_privados_padr = sapply(qtd_vinculos_priv, categorizar_vinculos_priv))

# Criar categorias para a quantidade de vínculos públicos
# Função para aplicar a lógica desejada
categorizar_vinculos_pub <- function(qtd) {
  if (qtd == 0) {
    return('Nenhum vínculo público')
  } else if (qtd == 1) {
    return('1 vínculo público')
  } else if (qtd == 2) {
    return('2 vínculos públicos')
  } else if (qtd == 3) {
    return('3 vínculos públicos')
  } else {
    return('4 ou mais vínculos públicos')
  }
}

# Aplicar a função à coluna e atribuir o resultado a uma nova coluna
df_tbl_final <- df_tbl_final %>%
  mutate(qtd_vinculos_pub_padr = sapply(qtd_vinculos_pub, categorizar_vinculos_pub))

# Criar categorias para a quantidade de vínculos no mesmo cargo
# Função para aplicar a lógica desejada
categorizar_vinculos_mesmo_cargo <- function(qtd) {
  if (qtd == 1) {
    return('1 vínculo')
  } else if (qtd == 2) {
    return('2 vínculos')
  } else {
    return('3 ou mais vínculos')
  }
}

# Aplicar a função à coluna e atribuir o resultado a uma nova coluna
df_tbl_final <- df_tbl_final %>%
  mutate(qtd_vinculos_mesmo_cargo_padr = sapply(qtd_vinculos_mesmo_cargo, categorizar_vinculos_mesmo_cargo))

# Criar categorias para a quantidade de vínculos na mesma carreira
# Função para aplicar a lógica desejada
categorizar_vinculos_mesma_carreira <- function(qtd) {
  if (qtd == 1) {
    return('1 vínculo')
  } else if (qtd == 2) {
    return('2 vínculos')
  } else {
    return('3 ou mais vínculos')
  }
}

# Aplicar a função à coluna e atribuir o resultado a uma nova coluna
df_tbl_final <- df_tbl_final %>%
  mutate(qtd_vinculos_mesma_carreira_padr = sapply(qtd_vinculos_mesma_carreira, categorizar_vinculos_mesma_carreira))

# Carregar bibliotecas necessárias
library(stringr)

# Convertendo a coluna 'cargo' para string
df_tbl_final$cargo <- as.character(df_tbl_final$cargo)

# Tratamento 1: uniformizando para letras minúsculas
df_tbl_final$cargo <- tolower(df_tbl_final$cargo)

df_tbl_final['cargo_antigo'] = df_tbl_final['cargo']

# Tratamento 2: Separando todos os que são professores

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("prof|pedago| mag 1|porof|prefessora|mag 1|prfoessor", ignore_case = FALSE))] <- "Professor"

# Tratamento 3: Separando os regentes de ensino
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("regente|ensino", ignore_case = FALSE))] <- "Regente de Ensino"

# Tratamento 4: Separando os administrativos

# Agente Administrativo
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("ag administrativo|agente de apoio administrativo|agente de at.adm|técnico adm|agente de ativ administrat|tecnico adm|asist tecnico adm|agente de adm|agente de ativ. adm|assessor p/ ass adm|agende administrativo|agente de administração|agente adm", ignore_case = FALSE))] <- "Agente Administrativo"

# Auxiliar Administrativo
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("apoio administrativo|assistente em administração|assitente administrativo|assit.adm|assistente adm|assistente de administração|assitente de adm|auxiliar tec adm|assessor p/ ass. adm.|acessor p ass adm|assessor p ass adm|auxilia adm|assistent tec adm|assist.adm|assistente de administracao|agente aux atvi adm|aux de administração|auxiliar de adm|ag adm auxiliar|assistente p/ ass adm geral|assessor adm|ag. atv. adm.|auxiliar de administração|assistente administrativo|auxiliar adm|aux adm|aux. adm| aux. de adm|aux. de adm.|assist. adm|assessor de adm", ignore_case = FALSE))] <- "Auxiliar Administrativo"

# Administrador
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("administrador", ignore_case = FALSE))] <- "Administrador"

# Tratamento 5: Separando os auxiliares por classe.. até a residual
# Auxiliar de Serviços
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("agende de serviços gerais|agente de serviços gerais|auxiliar oper. serv. diversos|auxiliar operacional serv diversos|auxilio de serviço|auxiliar operacional de serviços|auxilair de serviço|auxiiliar de serviços gerai|aux de serv gerais|aux.de servicos|auxiliar de sreviços|aux serv diversos|auxilar de serviços|auxiliar de serviço|aux de serviço|auxiliar de serviços|agente de serv auxiliares|auxiliar de servico|aux. serv. gerais|aux serviços gerais|auxiliar serviços|auxilair de serviços|auxíliar de serviços|auxiliar servicos gerais|auxilia de serviço gerais|auxiliar de sreviço|auxiliar operacional de serviços|auxiliar de seriços gerais|auxiliar de serv, gerais|auxíliar de serviços gerais|auxiliar de serv, gerais|auxilia de serviços gerais|auxiliar de seviços|auxiliar de servisos gerais|auxiliar de servições gerais|auxiliar de sreviço gerias|aux. de serviços|auxuliar de serviços|aux.serviços gerais|auxilair de serviços gerais|aux oper ser diversos|aux serv|aux. ser gerais|aux.de servico|aux. serviços|auxiliar de servços|auxiliar oper serv diversos|aux. ser. gerais|aux ser gerais|agente de servicos gerais", ignore_case = FALSE))] <- "Auxiliar de Serviços"

# Auxiliar Legislativo
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("leg", ignore_case = FALSE)) & str_detect(df_tbl_final$cargo, regex("aux", ignore_case = FALSE))] <- "Auxiliar Legislativo"

# Auxiliar de Limpeza Urbana
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("limp", ignore_case = FALSE)) & str_detect(df_tbl_final$cargo, regex("aux", ignore_case = FALSE))] <- "Auxiliar de Limpeza Urbana"

# Auxiliar de Limpeza Urbana (outras descrições)
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("agente limpeza publica|agente de limpeza|gari|garí", ignore_case = FALSE))] <- "Auxiliar de Limpeza Urbana"

# Auxiliar Técnico
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("tecn|técn", ignore_case = FALSE)) & str_detect(df_tbl_final$cargo, regex("aux", ignore_case = FALSE))] <- "Auxiliar Técnico"

# Auxiliar de Enfermagem
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("enf", ignore_case = FALSE)) & str_detect(df_tbl_final$cargo, regex("aux", ignore_case = FALSE))] <- "Auxiliar de Enfermagem"

# Auxiliar de Biblioteca
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("bib|cult", ignore_case = FALSE)) & str_detect(df_tbl_final$cargo, regex("aux", ignore_case = FALSE))] <- "Auxiliar de Biblioteca"

# Monitor de Creche
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("creche|ass. de berçário", ignore_case = FALSE))] <- "Monitor de Creche"

# Auxiliares - Residual
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("aux", ignore_case = FALSE))] <- "Auxiliares - Residual"

# Tratamento 6: Grupo da Enfermagem
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("enf", ignore_case = FALSE)) & str_detect(df_tbl_final$cargo, regex("tec|téc", ignore_case = FALSE))] <- "Técnico em Enfermagem"
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("tecnico de emfermagem", ignore_case = FALSE))] <- "Técnico em Enfermagem"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("enferme", ignore_case = FALSE))] <- "Enfermeiro"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("enf", ignore_case = FALSE))] <- "Assistente de Enfermagem"

# Tratamento 7: Técnicos
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("téc|tec", ignore_case = FALSE)) & str_detect(df_tbl_final$cargo, regex("médio|medio", ignore_case = FALSE))] <- "Técnicos - Nível Médio"
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("tec. niv. med. contabil|tenico de nivel medio", ignore_case = FALSE))] <- "Técnicos - Nível Médio"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("téc|tec", ignore_case = FALSE)) & str_detect(df_tbl_final$cargo, regex("sup", ignore_case = FALSE))] <- "Técnicos - Nível Superior"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("téc|tec", ignore_case = FALSE)) & str_detect(df_tbl_final$cargo, regex("legi", ignore_case = FALSE))] <- "Técnico Legislativo"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("cons", ignore_case = FALSE)) & str_detect(df_tbl_final$cargo, regex("tecni|técni|jur|leg|sup", ignore_case = FALSE))] <- "Consultor"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("téc|tec", ignore_case = FALSE)) & str_detect(df_tbl_final$cargo, regex("jud", ignore_case = FALSE))] <- "Técnico Judiciário"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("técn|tecn", ignore_case = FALSE)) & str_detect(df_tbl_final$cargo, regex("assi", ignore_case = FALSE))] <- "Assistente Técnico"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("parteira|apurador de dados|ilustrador|vendedor|diagramador|consultor de sistema|terapeuta ocupacional|relações publicas|ofic registro|agente documentarista|estatistico|tesoureiro|programador|almoxarife|publicitario|ag previdencia|tec.minist.delig.apoio|tec pol publicas e gestão gov|tec. de plan e desenv rural|técn|tecn|ag tec metrologico|tec.pol.publicas e gestao gov.|tec. de gestao organizacional|mecanico|laboratorista|mecanografo|mecânico|desenhista|encanador|soldador", ignore_case = FALSE))] <- "Técnicos Especializados"

# Tratamento 7: Peritos, Médicos, Dentistas, Psicólogos e Fisioterapeutas
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("perit", ignore_case = FALSE))] <- "Peritos"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("med|méd", ignore_case = FALSE))] <- "Médicos"
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("clin|veterinário|veterinario", ignore_case = FALSE))] <- "Médicos"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("odont|dentis", ignore_case = FALSE))] <- "Odontólogos"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("psi|psocologa", ignore_case = FALSE))] <- "Psicólogos"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("fisio", ignore_case = FALSE))] <- "Fisioterapeutas"

# Tratamento 8: Agentes de Saúde
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("agente com|agente de com|acs", ignore_case = FALSE))] <- "Agente Comunitário de Saúde"
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("agende com|agente de sa", ignore_case = FALSE))] <- "Agente Comunitário de Saúde"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("endemia|saude|saúde", ignore_case = FALSE))] <- "Agente de Combate à Endemias"

# Tratamento 9: Agentes Fiscais
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("trib", ignore_case = FALSE)) & str_detect(df_tbl_final$cargo, regex("fis", ignore_case = FALSE))] <- "Agentes Fiscais - Tributos"
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("agente de tributos e posturas|auditor fiscal|agente arrecadador|agente de arrecadação|ag.f.de trib.munic|agente fiscal merc|fiscal de arrecadação", ignore_case = FALSE))] <- "Agentes Fiscais - Tributos"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("fis", ignore_case = FALSE))] <- "Agentes Fiscais - Outros"

# Tratamento 10: Profissionais de Biblioteca
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("biblio", ignore_case = FALSE))] <- "Agentes de Biblioteca - Não auxiliares"
# Modificar a coluna 'cargo_novo' para "Atendentes" com base no padrão regex
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("atenden|recep|por", ignore_case = TRUE))] <- "Atendentes"

# Tratamento 11: Juízes, Advogados, Auditores, Arquitetos, Engenheiros, Arquivistas etc...
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("juiz|desembargador", ignore_case = FALSE))] <- "Juiz"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("adv", ignore_case = FALSE))] <- "Advogado"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("procurad", ignore_case = FALSE))] <- "Procurador"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("conta", ignore_case = FALSE))] <- "Contabilistas"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("analista", ignore_case = FALSE))] <- "Analistas"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("eng", ignore_case = FALSE)) & !str_detect(df_tbl_final$cargo, regex("engomadeira", ignore_case = FALSE))] <- "Engenheiro"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("arquit", ignore_case = FALSE))] <- "Arquiteto"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("arq", ignore_case = FALSE))] <- "Arquivista"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("bioq", ignore_case = FALSE))] <- "Bioquímico"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("bió|bio", ignore_case = FALSE))] <- "Biólogo"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("econ", ignore_case = FALSE))] <- "Economista"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("farm", ignore_case = FALSE))] <- "Farmacêutico"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("ofici", ignore_case = FALSE))] <- "Oficial de Justiça"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("dele", ignore_case = FALSE))] <- "Delegado"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("nutri", ignore_case = FALSE))] <- "Nutricionista"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("quim", ignore_case = FALSE))] <- "Químico"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("repor|locu|jornalista", ignore_case = FALSE))] <- "Jornalista"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("reda|revisor", ignore_case = FALSE))] <- "Redator"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("fot|geo|geó|géo", ignore_case = FALSE))] <- "Fotógrafos, Geólogos e Geógrafos"

# Tratamento 12: Profissionais da Educação - Exceto Professores
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("edu|escolar|pedagó", ignore_case = FALSE))] <- "Profissionais de Educação - Exceto Professores"

# Tratamento 13: Agentes de Segurança
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("investigador|vigilantte|vigia|vigilante|segu|guarda|sar|agente protetivo|agente de vifilância|agente de investigação|agente de vigilância|escrivao", ignore_case = FALSE))] <- "Agentes de Segurança"

# Tratamento 14: Motoristas e Merendeiras
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("moto|tratorista|condutor socorrista", ignore_case = FALSE))] <- "Motoristas"
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("meren|coz", ignore_case = FALSE))] <- "Cozinheiros"

# Tratamento 15: Assistente Legislativo
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("legis", ignore_case = FALSE))] <- "Assistente Legislativo"

# Tratamento 16: Assistente Social
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("social", ignore_case = FALSE))] <- "Assistente Social"
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("soci", ignore_case = FALSE))] <- "Sociólogo"

# Tratamento 17: Trabalhadores, Zeladores e Escriturários
df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("escri|escrtiturária|esvriturário", ignore_case = FALSE))] <- "Escriturário"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("protoco|paginado|costur|lavadeir|engoma|cabe|varr|pod|pol|serv|efetivo|asc|pintor|perf|borda|sani|jard|marc|carp|serviços diversos|serviços gerais|trab|servente|pedreiro|operario|operário|montador|monitor", ignore_case = FALSE))] <- "Trabalhador sem especificicação"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("zel", ignore_case = FALSE))] <- "Zelador"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("tel", ignore_case = FALSE))] <- "Telefonista"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("agente de", ignore_case = FALSE))] <- "Agentes Especializados"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("digi|dati|datrilógrafo", ignore_case = FALSE))] <- "Digitador"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("cov", ignore_case = FALSE))] <- "Coveiro"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("elet", ignore_case = FALSE))] <- "Eletricista"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("mus|mús|violinista", ignore_case = FALSE))] <- "Músico"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("asse|chefe|secre|super|assist", ignore_case = FALSE))] <- "Assessor"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("artí|arti", ignore_case = FALSE))] <- "Artífice"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("op|contro", ignore_case = FALSE))] <- "Operadores em Geral"

df_tbl_final$cargo[str_detect(df_tbl_final$cargo, regex("a.s.g|a.g.s|a.g.s.", ignore_case = FALSE))] <- "ASG"

#### Visão Geral: Classificados

classificados <- c("Professor", "Regente de Ensino", "Agente Administrativo", "Auxiliar Administrativo", "Administrador",
                   "Auxiliar de Serviços", "Auxiliar Legislativo", "Auxiliar de Limpeza Urbana", "Auxiliar Técnico",
                   "Auxiliar de Enfermagem", "Auxiliar de Biblioteca", "Monitor de Creche", "Auxiliares - Residual",
                   "Técnico em Enfermagem", "Enfermeiro", "Assistente de Enfermagem", "Técnicos - Nível Médio",
                   "Técnicos - Nível Superior", "Técnico Legislativo", "Consultor", "Assistente Técnico", "Técnico Judiciário",
                   "Técnicos Especializados", "Peritos", "Médicos", "Odontólogos", "Psicólogos", "Fisioterapeutas", "Agente Comunitário de Saúde",
                   "Agente de Combate à Endemias", "Agentes Fiscais - Tributos", "Agentes Fiscais - Outros", "Agentes de Biblioteca - Não auxiliares",
                   "Atendentes", "Juiz", "Advogado", "Procurador", "Defensor", "Auditor", "Contabilistas", "Analistas", "Engenheiro", "Arquiteto", "Arquivista",
                   "Profissionais de Educação - Exceto Professores", "Agentes de Segurança", "Motoristas", "Cozinheiros",
                   "Assistente Legislativo", "Assistente Social", "Sociólogo", "Bioquímico", "Biólogo", "Economista", "Escriturário",
                   "Trabalhador sem especificicação", "Telefonista", "Agentes Especializados", "Zelador", "Farmacêutico", "Oficial de Justiça",
                   "Digitador", "Delegado", "Nutricionista", "Coveiro", "Eletricista", "Músico", "Assessor", "Artífice", "Químico", "Operadores em Geral",
                   "Jornalista", "Redator", "ASG", "Fotógrafos, Geológos e Geógrafos")

class(classificados)

# Create a copy of the DataFrame to avoid modifying the original data
df_modified <- df_tbl_final

# Identify rows where "cargo" is not in "classificados"
mask <- !df_modified$cargo %in% classificados

mask
#### Tratamento 18: Residual - Classe: Outros. Profissionais em que não consegui identificar o nível de comprlexidade (Médio, Superior ou Fundamental e, de acordo com a quantidade deles, não valeu a pena criar uma categoria própria para eles

# Atualiza as linhas onde a condição é verdadeira com o valor "Outros"
df_tbl_final$cargo[mask] <- "Outros"

# Contagem das ocorrências de cada conteúdo na coluna "descricao"
contagem_cargo <- table(df_tbl_final$cargo)

# Lista dos valores que ocorrem mais de 50 vezes
cargos_mais_de_50 <- names(contagem_cargo[contagem_cargo > 50])

# Mapeando os valores para "Outros" se ocorrerem menos de 50 vezes
df_tbl_final$PARA_cargo <- sapply(df_tbl_final$cargo, function(x) if (x %in% cargos_mais_de_50) x else "Outros")

colnames(df_tbl_final)

library(writexl)
# Exportar para um arquivo Excel
# write_xlsx(df_tbl_final, path = "Base_Conferencia_6_R.xlsx")


# ------------------------------------------------------------ CONFERIDO PERFEITAMENTE ATÉ AQUI -------------------------------------------------------------

#### Código de avaliação do valor do benefício em relação ao salário mínimo
library(lubridate)
library(tidyr)

# Importar tabela auxiliar de salário mínimo
df_salariominimo <- read_excel('Salario_Minimo.xlsx')
head(df_salariominimo, 15)

print(class(df_tbl_final$datapublicacaoconcessaobeneficio))
print(class(df_salariominimo$`Data Inicio`))

dim(df_tbl_final)

df_tbl_final <- df_tbl_final %>% drop_na(datapublicacaoconcessaobeneficio)

dim(df_tbl_final)

# Convertendo as colunas de data para o tipo datetime, se necessário
df_tbl_final$datapublicacaoconcessaobeneficio <- as_datetime(df_tbl_final$datapublicacaoconcessaobeneficio)
df_salariominimo$`Data Inicio` <- as_datetime(df_salariominimo$`Data Inicio`)
df_salariominimo$`Data Fim` <- as_datetime(df_salariominimo$`Data Fim`)

# Ordenando os DataFrames pela coluna de data
df_tbl_final <- df_tbl_final %>% arrange(datapublicacaoconcessaobeneficio)
df_salariominimo <- df_salariominimo %>% arrange(`Data Inicio`)

# Realizando a mesclagem dos DataFrames
# install.packages("fuzzyjoin")
library(fuzzyjoin)
df_tbl_final <- df_tbl_final %>%
  fuzzy_left_join(df_salariominimo, 
                  by = c("datapublicacaoconcessaobeneficio" = "Data Inicio",
                         "datapublicacaoconcessaobeneficio" = "Data Fim"),
                  match_fun = list(`>=`, `<=`))

# Filtrando os registros e selecionando as colunas desejadas
df_tbl_final <- df_tbl_final %>%
  filter(!is.na(`Valor em reais`))

# Se desejar, você pode remover linhas duplicadas, se houver
df_tbl_final <- df_tbl_final %>% distinct()

# Criando o novo campo com base na lógica fornecida
df_tbl_final <- df_tbl_final %>%
  mutate(indicador_sm = ifelse(valor_Proventos <= `Valor em reais`, 1, 0))


library(writexl)
# Exportar para um arquivo Excel
write_xlsx(df_merged, path = "Base_Conferencia_7_R.xlsx")

conexao <- dbConnect(SQLite(), db_path)

# Use o método dbWriteTable para escrever o DataFrame no banco de dados
# Substitua 'tabela_final' pelo nome que deseja para a tabela
dbWriteTable(conexao, 'tabela_com_cargos_e_sm', df_merged, overwrite = TRUE, row.names = FALSE)

dbDisconnect(conexao)

dados_df <- df_tbl_final

# Tratamento 1: uniformizando para letras minúsculas e em uma nova coluna

dados_df$proventos <- tolower(dados_df$nome_Proventos)

# Tratamento 3: Tentando o split no subdataframe com o expand.
# Retirando os "NA" e os substituindo por "zero"

df_proventos <- dados_df[, c("protocolo", "proventos")] %>%
  filter(!is.na(proventos)) %>%
  mutate(proventos = as.character(proventos)) %>%
  separate(proventos, into = paste0("col", 1:10), sep = "/", fill = "right", remove = FALSE, convert = FALSE) %>%
  replace_na(list(col1 = "0", col2 = "0", col3 = "0", col4 = "0", col5 = "0", col6 = "0", col7 = "0", col8 = "0", col9 = "0", col10 = "0"))

# Tratamento 3.1: Tratando as subcolunas obtidas com o split

# Movimento na coluna 1:

# Reclassificar os valores na coluna col1

df_proventos$col1 <- ifelse(grepl("hora|6 parte|adicional t|adts|temp.serviço|adic.temp.serv |sexta parte|temp. serviço|anueni|quadri|quiquenio|quinq|anuenio|anuênio|tempo ser|tempo de serviço|tmepo ser|tempo de srv|adicional por tempo de ser|adic. por tempo de seriço|adic. temp.serv.|adic. por tempo de seviço|adicionais por tempo de serv.|adic.por tempo de ser viços|anuencio|adicionais tempo srrviço|adicional de tempo de ser|adicional de tempo de servico|trienio|quiqnenio|trmpo serviço|adicional por tempo de servico|adic. por tempod e serviço|tmepo de serviço|tempos serviço|adc. tempo|de serviço inativo|adicionais por tempode serviço|tempo de  serviços|temp. de serviço|adicional de t. serviço|acréscimo por ano excedido 10%|adc. tem. sev.|quiqu|trênio|tempo de servicos|tempo de contribuição|triênio|adic. temp. de serviço|hora|6 parte|adicional t|adts|temp.serviço|adic.temp.serv |sexta parte|temp. serviço|anueni|quadri|quiquenio|quinq|anuenio|anuênio|tempo ser|tempo de serviço|tmepo ser|tempo de srv|ad. t.", df_proventos$col1, ignore.case = TRUE), "ATS", df_proventos$col1)
df_proventos$col1 <- ifelse(grepl("vent. pess|vant|vpni|v.pes|v. pes|vatnatagem|abono|diferença do pccr|a.pess|a.pes|vent. pess|vant|vpni|v.pes|v. pes|vatnatagem|abono|diferença do pccr|a.pess|a.pes|complement", df_proventos$col1, ignore.case = TRUE), "Vantagem Pessoal", df_proventos$col1)
df_proventos$col1 <- ifelse(grepl("pensão|provent|outros acresc. inatividade|provnto|aposent|prov|média|media|resultado|propor|rpovento|prvento|posent", df_proventos$col1, ignore.case = TRUE), "Proventos", df_proventos$col1)
df_proventos$col1 <- ifelse(grepl("sal. incorp.|vencimento|vecimento|piso nacional|venc.|vertical|dif pccr|diferença pccr|sal.incorp|venvi|salári|salari|remun|base|subsidio|subsídio|venecimento", df_proventos$col1, ignore.case = TRUE), "Vencimento", df_proventos$col1)
df_proventos$col1 <- ifelse(grepl("insalubri|isalu|insalub", df_proventos$col1, ignore.case = TRUE), "Insalubridade", df_proventos$col1)
df_proventos$col1 <- ifelse(grepl("antec", df_proventos$col1, ignore.case = TRUE), "Antecipação de Aumento", df_proventos$col1)
df_proventos$col1 <- ifelse(grepl("represen|rep.|represetação", df_proventos$col1, ignore.case = TRUE), "Representação", df_proventos$col1)
df_proventos$col1 <- ifelse(grepl("gifs|qualif|titul|especialização", df_proventos$col1, ignore.case = TRUE), "Adc. Qualificação", df_proventos$col1)
df_proventos$col1 <- ifelse(grepl("adicional|risco de vida|pó de giz|acrescimo 20|produção e prod.|outros acrescimos pecuniarios|periculosidade|adicionais de permanência|gead|adic. de permanencia|regencia de classe|serv. extra.|produção prod.|serviço extra incorporado|horas|outros acresc.pecuniarios|bolsa avaliacao desemp.docente|gal|assidui|grat|magistério|magisterio|vpnr|complemento|produtivid|gcex|gpcex|noturno|adic. not.|gae|jornada ampliada|bolsa de sempenho fiscal|efetivo desempenho|adi.jorn.ampl|risco de vida|pó de giz|acrescimo 20|produção e prod.|outros acrescimos pecuniarios|periculosidade|adicionais de permanência|gead|adic. de permanencia|regencia de classe|serv. extra.|produção prod.|serviço extra incorporado|horas|outros acresc.pecuniarios|bolsa avaliacao desemp.docente|gal|assidui|grat|magistério|magisterio|vpnr|complemento|produtivid|gcex|gpcex|noturno|adic. not.|gaj|g.a.p|gap|gati|gshu|gdp|grat|gal|noturno|vpnr|prod. s. bucal", df_proventos$col1, ignore.case = TRUE), "Gratificação", df_proventos$col1)
df_proventos$col1 <- ifelse(grepl("ajuda de custo|auxilio saude", df_proventos$col1, ignore.case = TRUE), "Indenizatorio", df_proventos$col1)
df_proventos$col1 <- ifelse(grepl("^[0-9]|90", df_proventos$col1, ignore.case = TRUE), "0", df_proventos$col1)
# Definindo valores que não se enquadram nas categorias acima como "0"
df_proventos$col1 <- ifelse(!grepl("Proventos|Ind|Rep|ATS|Venc|Insa|Qualif|Antec|Vant|Grat", df_proventos$col1, ignore.case = TRUE), "0", df_proventos$col1)

# Movimento na coluna 2:

df_proventos$col2 <- ifelse(grepl("hora|6 parte|adicional t|adts|temp.serviço|adic.temp.serv |sexta parte|temp. serviço|anueni|quadri|quiquenio|quinq|anuenio|anuênio|tempo ser|tempo de serviço|tmepo ser|tempo de srv|adicional por tempo de ser|adic. por tempo de seriço|adic. temp.serv.|adic. por tempo de seviço|adicionais por tempo de serv.|adic.por tempo de ser viços|anuencio|adicionais tempo srrviço|adicional de tempo de ser|adicional de tempo de servico|trienio|quiqnenio|trmpo serviço|adicional por tempo de servico|adic. por tempod e serviço|tmepo de serviço|tempos serviço|adc. tempo|de serviço inativo|adicionais por tempode serviço|tempo de  serviços|temp. de serviço|adicional de t. serviço|acréscimo por ano excedido 10%|adc. tem. sev.|quiqu|trênio|tempo de servicos|tempo de contribuição|triênio|adic. temp. de serviço|hora|6 parte|adicional t|adts|temp.serviço|adic.temp.serv |sexta parte|temp. serviço|anueni|quadri|quiquenio|quinq|anuenio|anuênio|tempo ser|tempo de serviço|tmepo ser|tempo de srv|ad. t.", df_proventos$col2, ignore.case = TRUE), "ATS", df_proventos$col2)
df_proventos$col2 <- ifelse(grepl("vent. pess|vant|vpni|v.pes|v. pes|vatnatagem|abono|diferença do pccr|a.pess|a.pes|vent. pess|vant|vpni|v.pes|v. pes|vatnatagem|abono|diferença do pccr|a.pess|a.pes|complement", df_proventos$col2, ignore.case = TRUE), "Vantagem Pessoal", df_proventos$col2)
df_proventos$col2 <- ifelse(grepl("pensão|provent|outros acresc. inatividade|provnto|aposent|prov|média|media|resultado|propor|rpovento|prvento|posent", df_proventos$col2, ignore.case = TRUE), "Proventos", df_proventos$col2)
df_proventos$col2 <- ifelse(grepl("sal. incorp.|vencimento|vecimento|piso nacional|venc.|vertical|dif pccr|diferença pccr|sal.incorp|venvi|salári|salari|remun|base|subsidio|subsídio|venecimento", df_proventos$col2, ignore.case = TRUE), "Vencimento", df_proventos$col2)
df_proventos$col2 <- ifelse(grepl("insalubri|isalu|insalub", df_proventos$col2, ignore.case = TRUE), "Insalubridade", df_proventos$col2)
df_proventos$col2 <- ifelse(grepl("antec", df_proventos$col2, ignore.case = TRUE), "Antecipação de Aumento", df_proventos$col2)
df_proventos$col2 <- ifelse(grepl("represen|rep.|represetação", df_proventos$col2, ignore.case = TRUE), "Representação", df_proventos$col2)
df_proventos$col2 <- ifelse(grepl("gifs|qualif|titul|especialização", df_proventos$col2, ignore.case = TRUE), "Adc. Qualificação", df_proventos$col2)
df_proventos$col2 <- ifelse(grepl("adicional|risco de vida|pó de giz|acrescimo 20|produção e prod.|outros acrescimos pecuniarios|periculosidade|adicionais de permanência|gead|adic. de permanencia|regencia de classe|serv. extra.|produção prod.|serviço extra incorporado|horas|outros acresc.pecuniarios|bolsa avaliacao desemp.docente|gal|assidui|grat|magistério|magisterio|vpnr|complemento|produtivid|gcex|gpcex|noturno|adic. not.|gae|jornada ampliada|bolsa de sempenho fiscal|efetivo desempenho|adi.jorn.ampl|risco de vida|pó de giz|acrescimo 20|produção e prod.|outros acrescimos pecuniarios|periculosidade|adicionais de permanência|gead|adic. de permanencia|regencia de classe|serv. extra.|produção prod.|serviço extra incorporado|horas|outros acresc.pecuniarios|bolsa avaliacao desemp.docente|gal|assidui|grat|magistério|magisterio|vpnr|complemento|produtivid|gcex|gpcex|noturno|adic. not.|gaj|g.a.p|gap|gati|gshu|gdp|grat|gal|noturno|vpnr|prod. s. bucal", df_proventos$col2, ignore.case = TRUE), "Gratificação", df_proventos$col2)
df_proventos$col2 <- ifelse(grepl("ajuda de custo|auxilio saude", df_proventos$col2, ignore.case = TRUE), "Indenizatorio", df_proventos$col2)
df_proventos$col2 <- ifelse(grepl("^[0-9]|90", df_proventos$col2, ignore.case = TRUE), "0", df_proventos$col2)
# Definindo valores que não se enquadram nas categorias acima como "0"
df_proventos$col2 <- ifelse(!grepl("Proventos|Ind|Rep|ATS|Venc|Insa|Qualif|Antec|Vant|Grat", df_proventos$col2, ignore.case = TRUE), "0", df_proventos$col2)

# Movimento na coluna 3:

df_proventos$col3 <- ifelse(grepl("hora|6 parte|adicional t|adts|temp.serviço|adic.temp.serv |sexta parte|temp. serviço|anueni|quadri|quiquenio|quinq|anuenio|anuênio|tempo ser|tempo de serviço|tmepo ser|tempo de srv|adicional por tempo de ser|adic. por tempo de seriço|adic. temp.serv.|adic. por tempo de seviço|adicionais por tempo de serv.|adic.por tempo de ser viços|anuencio|adicionais tempo srrviço|adicional de tempo de ser|adicional de tempo de servico|trienio|quiqnenio|trmpo serviço|adicional por tempo de servico|adic. por tempod e serviço|tmepo de serviço|tempos serviço|adc. tempo|de serviço inativo|adicionais por tempode serviço|tempo de  serviços|temp. de serviço|adicional de t. serviço|acréscimo por ano excedido 10%|adc. tem. sev.|quiqu|trênio|tempo de servicos|tempo de contribuição|triênio|adic. temp. de serviço|hora|6 parte|adicional t|adts|temp.serviço|adic.temp.serv |sexta parte|temp. serviço|anueni|quadri|quiquenio|quinq|anuenio|anuênio|tempo ser|tempo de serviço|tmepo ser|tempo de srv|ad. t.", df_proventos$col3, ignore.case = TRUE), "ATS", df_proventos$col3)
df_proventos$col3 <- ifelse(grepl("vent. pess|vant|vpni|v.pes|v. pes|vatnatagem|abono|diferença do pccr|a.pess|a.pes|vent. pess|vant|vpni|v.pes|v. pes|vatnatagem|abono|diferença do pccr|a.pess|a.pes|complement", df_proventos$col3, ignore.case = TRUE), "Vantagem Pessoal", df_proventos$col3)
df_proventos$col3 <- ifelse(grepl("pensão|provent|outros acresc. inatividade|provnto|aposent|prov|média|media|resultado|propor|rpovento|prvento|posent", df_proventos$col3, ignore.case = TRUE), "Proventos", df_proventos$col3)
df_proventos$col3 <- ifelse(grepl("sal. incorp.|vencimento|vecimento|piso nacional|venc.|vertical|dif pccr|diferença pccr|sal.incorp|venvi|salári|salari|remun|base|subsidio|subsídio|venecimento", df_proventos$col3, ignore.case = TRUE), "Vencimento", df_proventos$col3)
df_proventos$col3 <- ifelse(grepl("insalubri|isalu|insalub", df_proventos$col3, ignore.case = TRUE), "Insalubridade", df_proventos$col3)
df_proventos$col3 <- ifelse(grepl("antec", df_proventos$col3, ignore.case = TRUE), "Antecipação de Aumento", df_proventos$col3)
df_proventos$col3 <- ifelse(grepl("represen|rep.|represetação", df_proventos$col3, ignore.case = TRUE), "Representação", df_proventos$col3)
df_proventos$col3 <- ifelse(grepl("gifs|qualif|titul|especialização", df_proventos$col3, ignore.case = TRUE), "Adc. Qualificação", df_proventos$col3)
df_proventos$col3 <- ifelse(grepl("adicional|risco de vida|pó de giz|acrescimo 20|produção e prod.|outros acrescimos pecuniarios|periculosidade|adicionais de permanência|gead|adic. de permanencia|regencia de classe|serv. extra.|produção prod.|serviço extra incorporado|horas|outros acresc.pecuniarios|bolsa avaliacao desemp.docente|gal|assidui|grat|magistério|magisterio|vpnr|complemento|produtivid|gcex|gpcex|noturno|adic. not.|gae|jornada ampliada|bolsa de sempenho fiscal|efetivo desempenho|adi.jorn.ampl|risco de vida|pó de giz|acrescimo 20|produção e prod.|outros acrescimos pecuniarios|periculosidade|adicionais de permanência|gead|adic. de permanencia|regencia de classe|serv. extra.|produção prod.|serviço extra incorporado|horas|outros acresc.pecuniarios|bolsa avaliacao desemp.docente|gal|assidui|grat|magistério|magisterio|vpnr|complemento|produtivid|gcex|gpcex|noturno|adic. not.|gaj|g.a.p|gap|gati|gshu|gdp|grat|gal|noturno|vpnr|prod. s. bucal", df_proventos$col3, ignore.case = TRUE), "Gratificação", df_proventos$col3)
df_proventos$col3 <- ifelse(grepl("ajuda de custo|auxilio saude", df_proventos$col3, ignore.case = TRUE), "Indenizatorio", df_proventos$col3)
df_proventos$col3 <- ifelse(grepl("^[0-9]|90", df_proventos$col3, ignore.case = TRUE), "0", df_proventos$col3)
# Definindo valores que não se enquadram nas categorias acima como "0"
df_proventos$col3 <- ifelse(!grepl("Proventos|Ind|Rep|ATS|Venc|Insa|Qualif|Antec|Vant|Grat", df_proventos$col3, ignore.case = TRUE), "0", df_proventos$col3)

# Movimento na coluna 4:

df_proventos$col4 <- ifelse(grepl("hora|6 parte|adicional t|adts|temp.serviço|adic.temp.serv |sexta parte|temp. serviço|anueni|quadri|quiquenio|quinq|anuenio|anuênio|tempo ser|tempo de serviço|tmepo ser|tempo de srv|adicional por tempo de ser|adic. por tempo de seriço|adic. temp.serv.|adic. por tempo de seviço|adicionais por tempo de serv.|adic.por tempo de ser viços|anuencio|adicionais tempo srrviço|adicional de tempo de ser|adicional de tempo de servico|trienio|quiqnenio|trmpo serviço|adicional por tempo de servico|adic. por tempod e serviço|tmepo de serviço|tempos serviço|adc. tempo|de serviço inativo|adicionais por tempode serviço|tempo de  serviços|temp. de serviço|adicional de t. serviço|acréscimo por ano excedido 10%|adc. tem. sev.|quiqu|trênio|tempo de servicos|tempo de contribuição|triênio|adic. temp. de serviço|hora|6 parte|adicional t|adts|temp.serviço|adic.temp.serv |sexta parte|temp. serviço|anueni|quadri|quiquenio|quinq|anuenio|anuênio|tempo ser|tempo de serviço|tmepo ser|tempo de srv|ad. t.|serviço|serviço 30%|serviço 20%|serviço 10%|adc. tempo de serv|quinq|anuenio|anuênio|tempo e ser|tempo de serviç|sexta parte", df_proventos$col4, ignore.case = TRUE), "ATS", df_proventos$col4)
df_proventos$col4 <- ifelse(grepl("vent. pess|vant|vpni|v.pes|v. pes|vatnatagem|abono|diferença do pccr|a.pess|a.pes|vent. pess|vant|vpni|v.pes|v. pes|vatnatagem|abono|diferença do pccr|a.pess|a.pes|complement", df_proventos$col4, ignore.case = TRUE), "Vantagem Pessoal", df_proventos$col4)
df_proventos$col4 <- ifelse(grepl("pensão|provent|outros acresc. inatividade|provnto|aposent|prov|média|media|resultado|propor|rpovento|prvento|posent", df_proventos$col4, ignore.case = TRUE), "Proventos", df_proventos$col4)
df_proventos$col4 <- ifelse(grepl("sal. incorp.|vencimento|vecimento|piso nacional|venc.|vertical|dif pccr|diferença pccr|sal.incorp|venvi|salári|salari|remun|base|subsidio|subsídio|venecimento", df_proventos$col4, ignore.case = TRUE), "Vencimento", df_proventos$col4)
df_proventos$col4 <- ifelse(grepl("insalubri|isalu|insalub", df_proventos$col4, ignore.case = TRUE), "Insalubridade", df_proventos$col4)
df_proventos$col4 <- ifelse(grepl("antec", df_proventos$col4, ignore.case = TRUE), "Antecipação de Aumento", df_proventos$col4)
df_proventos$col4 <- ifelse(grepl("represen|rep.|represetação", df_proventos$col4, ignore.case = TRUE), "Representação", df_proventos$col4)
df_proventos$col4 <- ifelse(grepl("gifs|qualif|titul|especialização", df_proventos$col4, ignore.case = TRUE), "Adc. Qualificação", df_proventos$col4)
df_proventos$col4 <- ifelse(grepl("adicional|risco de vida|pó de giz|acrescimo 20|produção e prod.|outros acrescimos pecuniarios|periculosidade|adicionais de permanência|gead|adic. de permanencia|regencia de classe|serv. extra.|produção prod.|serviço extra incorporado|horas|outros acresc.pecuniarios|bolsa avaliacao desemp.docente|gal|assidui|grat|magistério|magisterio|vpnr|complemento|produtivid|gcex|gpcex|noturno|adic. not.|gae|jornada ampliada|bolsa de sempenho fiscal|efetivo desempenho|adi.jorn.ampl|risco de vida|pó de giz|acrescimo 20|produção e prod.|outros acrescimos pecuniarios|periculosidade|adicionais de permanência|gead|adic. de permanencia|regencia de classe|serv. extra.|produção prod.|serviço extra incorporado|horas|outros acresc.pecuniarios|bolsa avaliacao desemp.docente|gal|assidui|grat|magistério|magisterio|vpnr|complemento|produtivid|gcex|gpcex|noturno|adic. not.|gaj|g.a.p|gap|gati|gshu|gdp|grat|gal|noturno|vpnr|prod. s. bucal", df_proventos$col4, ignore.case = TRUE), "Gratificação", df_proventos$col4)
df_proventos$col4 <- ifelse(grepl("ajuda de custo|auxilio saude", df_proventos$col4, ignore.case = TRUE), "Indenizatorio", df_proventos$col4)
df_proventos$col4 <- ifelse(grepl("^[0-9]|90", df_proventos$col4, ignore.case = TRUE), "0", df_proventos$col4)
# Definindo valores que não se enquadram nas categorias acima como "0"
df_proventos$col4 <- ifelse(!grepl("Proventos|Ind|Rep|ATS|Venc|Insa|Qualif|Antec|Vant|Grat", df_proventos$col4, ignore.case = TRUE), "0", df_proventos$col4)

# Movimento na coluna 5:

df_proventos$col5 <- ifelse(grepl("hora|6 parte|adicional t|adts|temp.serviço|adic.temp.serv |sexta parte|temp. serviço|anueni|quadri|quiquenio|quinq|anuenio|anuênio|tempo ser|tempo de serviço|tmepo ser|tempo de srv|adicional por tempo de ser|adic. por tempo de seriço|adic. temp.serv.|adic. por tempo de seviço|adicionais por tempo de serv.|adic.por tempo de ser viços|anuencio|adicionais tempo srrviço|adicional de tempo de ser|adicional de tempo de servico|trienio|quiqnenio|trmpo serviço|adicional por tempo de servico|adic. por tempod e serviço|tmepo de serviço|tempos serviço|adc. tempo|de serviço inativo|adicionais por tempode serviço|tempo de  serviços|temp. de serviço|adicional de t. serviço|acréscimo por ano excedido 10%|adc. tem. sev.|quiqu|trênio|tempo de servicos|tempo de contribuição|triênio|adic. temp. de serviço|hora|6 parte|adicional t|adts|temp.serviço|adic.temp.serv |sexta parte|temp. serviço|anueni|quadri|quiquenio|quinq|anuenio|anuênio|tempo ser|tempo de serviço|tmepo ser|tempo de srv|ad. t.", df_proventos$col5, ignore.case = TRUE), "ATS", df_proventos$col5)
df_proventos$col5 <- ifelse(grepl("vent. pess|vant|vpni|v.pes|v. pes|vatnatagem|abono|diferença do pccr|a.pess|a.pes|vent. pess|vant|vpni|v.pes|v. pes|vatnatagem|abono|diferença do pccr|a.pess|a.pes|complement", df_proventos$col5, ignore.case = TRUE), "Vantagem Pessoal", df_proventos$col5)
df_proventos$col5 <- ifelse(grepl("pensão|provent|outros acresc. inatividade|provnto|aposent|prov|média|media|resultado|propor|rpovento|prvento|posent", df_proventos$col5, ignore.case = TRUE), "Proventos", df_proventos$col5)
df_proventos$col5 <- ifelse(grepl("sal. incorp.|vencimento|vecimento|piso nacional|venc.|vertical|dif pccr|diferença pccr|sal.incorp|venvi|salári|salari|remun|base|subsidio|subsídio|venecimento", df_proventos$col5, ignore.case = TRUE), "Vencimento", df_proventos$col5)
df_proventos$col5 <- ifelse(grepl("insalubri|isalu|insalub", df_proventos$col5, ignore.case = TRUE), "Insalubridade", df_proventos$col5)
df_proventos$col5 <- ifelse(grepl("antec", df_proventos$col5, ignore.case = TRUE), "Antecipação de Aumento", df_proventos$col5)
df_proventos$col5 <- ifelse(grepl("represen|rep.|represetação", df_proventos$col5, ignore.case = TRUE), "Representação", df_proventos$col5)
df_proventos$col5 <- ifelse(grepl("gifs|qualif|titul|especialização", df_proventos$col5, ignore.case = TRUE), "Adc. Qualificação", df_proventos$col5)
df_proventos$col5 <- ifelse(grepl("adicional|risco de vida|pó de giz|acrescimo 20|produção e prod.|outros acrescimos pecuniarios|periculosidade|adicionais de permanência|gead|adic. de permanencia|regencia de classe|serv. extra.|produção prod.|serviço extra incorporado|horas|outros acresc.pecuniarios|bolsa avaliacao desemp.docente|gal|assidui|grat|magistério|magisterio|vpnr|complemento|produtivid|gcex|gpcex|noturno|adic. not.|gae|jornada ampliada|bolsa de sempenho fiscal|efetivo desempenho|adi.jorn.ampl|risco de vida|pó de giz|acrescimo 20|produção e prod.|outros acrescimos pecuniarios|periculosidade|adicionais de permanência|gead|adic. de permanencia|regencia de classe|serv. extra.|produção prod.|serviço extra incorporado|horas|outros acresc.pecuniarios|bolsa avaliacao desemp.docente|gal|assidui|grat|magistério|magisterio|vpnr|complemento|produtivid|gcex|gpcex|noturno|adic. not.|gaj|g.a.p|gap|gati|gshu|gdp|grat|gal|noturno|vpnr|prod. s. bucal", df_proventos$col5, ignore.case = TRUE), "Gratificação", df_proventos$col5)
df_proventos$col5 <- ifelse(grepl("ajuda de custo", df_proventos$col5, ignore.case = TRUE), "Indenizatorio", df_proventos$col5)
df_proventos$col5 <- ifelse(grepl("^[0-9]|90", df_proventos$col5, ignore.case = TRUE), "0", df_proventos$col5)
# Definindo valores que não se enquadram nas categorias acima como "0"
df_proventos$col5 <- ifelse(!grepl("Proventos|Ind|Rep|ATS|Venc|Insa|Qualif|Antec|Vant|Grat", df_proventos$col5, ignore.case = TRUE), "0", df_proventos$col5)

# Movimento na coluna 6:

df_proventos$col6 <- ifelse(grepl("hora|6 parte|adicional t|adts|temp.serviço|adic.temp.serv |sexta parte|temp. serviço|anueni|quadri|quiquenio|quinq|anuenio|anuênio|tempo ser|tempo de serviço|tmepo ser|tempo de srv|adicional por tempo de ser|adic. por tempo de seriço|adic. temp.serv.|adic. por tempo de seviço|adicionais por tempo de serv.|adic.por tempo de ser viços|anuencio|adicionais tempo srrviço|adicional de tempo de ser|adicional de tempo de servico|trienio|quiqnenio|trmpo serviço|adicional por tempo de servico|adic. por tempod e serviço|tmepo de serviço|tempos serviço|adc. tempo|de serviço inativo|adicionais por tempode serviço|tempo de  serviços|temp. de serviço|adicional de t. serviço|acréscimo por ano excedido 10%|adc. tem. sev.|quiqu|trênio|tempo de servicos|tempo de contribuição|triênio|adic. temp. de serviço|hora|6 parte|adicional t|adts|temp.serviço|adic.temp.serv |sexta parte|temp. serviço|anueni|quadri|quiquenio|quinq|anuenio|anuênio|tempo ser|tempo de serviço|tmepo ser|tempo de srv|ad. t.", df_proventos$col6, ignore.case = TRUE), "ATS", df_proventos$col6)
df_proventos$col6 <- ifelse(grepl("vent. pess|vant|vpni|v.pes|v. pes|vatnatagem|abono|diferença do pccr|a.pess|a.pes|vent. pess|vant|vpni|v.pes|v. pes|vatnatagem|abono|diferença do pccr|a.pess|a.pes|complement", df_proventos$col6, ignore.case = TRUE), "Vantagem Pessoal", df_proventos$col6)
df_proventos$col6 <- ifelse(grepl("pensão|provent|outros acresc. inatividade|provnto|aposent|prov|média|media|resultado|propor|rpovento|prvento|posent", df_proventos$col6, ignore.case = TRUE), "Proventos", df_proventos$col6)
df_proventos$col6 <- ifelse(grepl("sal. incorp.|vencimento|vecimento|piso nacional|venc.|vertical|dif pccr|diferença pccr|sal.incorp|venvi|salári|salari|remun|base|subsidio|subsídio|venecimento", df_proventos$col6, ignore.case = TRUE), "Vencimento", df_proventos$col6)
df_proventos$col6 <- ifelse(grepl("insalubri|isalu|insalub", df_proventos$col6, ignore.case = TRUE), "Insalubridade", df_proventos$col6)
df_proventos$col6 <- ifelse(grepl("antec", df_proventos$col6, ignore.case = TRUE), "Antecipação de Aumento", df_proventos$col6)
df_proventos$col6 <- ifelse(grepl("represen|rep.|represetação", df_proventos$col6, ignore.case = TRUE), "Representação", df_proventos$col6)
df_proventos$col6 <- ifelse(grepl("gifs|qualif|titul|especialização", df_proventos$col6, ignore.case = TRUE), "Adc. Qualificação", df_proventos$col6)
df_proventos$col6 <- ifelse(grepl("adicional|risco de vida|pó de giz|acrescimo 20|produção e prod.|outros acrescimos pecuniarios|periculosidade|adicionais de permanência|gead|adic. de permanencia|regencia de classe|serv. extra.|produção prod.|serviço extra incorporado|horas|outros acresc.pecuniarios|bolsa avaliacao desemp.docente|gal|assidui|grat|magistério|magisterio|vpnr|complemento|produtivid|gcex|gpcex|noturno|adic. not.|gae|jornada ampliada|bolsa de sempenho fiscal|efetivo desempenho|adi.jorn.ampl|risco de vida|pó de giz|acrescimo 20|produção e prod.|outros acrescimos pecuniarios|periculosidade|adicionais de permanência|gead|adic. de permanencia|regencia de classe|serv. extra.|produção prod.|serviço extra incorporado|horas|outros acresc.pecuniarios|bolsa avaliacao desemp.docente|gal|assidui|grat|magistério|magisterio|vpnr|complemento|produtivid|gcex|gpcex|noturno|adic. not.|gaj|g.a.p|gap|gati|gshu|gdp|grat|gal|noturno|vpnr|prod. s. bucal", df_proventos$col6, ignore.case = TRUE), "Gratificação", df_proventos$col6)
df_proventos$col6 <- ifelse(grepl("ajuda de custo", df_proventos$col6, ignore.case = TRUE), "Indenizatorio", df_proventos$col6)
df_proventos$col6 <- ifelse(grepl("^[0-9]|90", df_proventos$col6, ignore.case = TRUE), "0", df_proventos$col6)
# Definindo valores que não se enquadram nas categorias acima como "0"
df_proventos$col6 <- ifelse(!grepl("Proventos|Ind|Rep|ATS|Venc|Insa|Qualif|Antec|Vant|Grat", df_proventos$col6, ignore.case = TRUE), "0", df_proventos$col6)

# Movimento na coluna 7:

df_proventos$col7 <- ifelse(grepl("hora|6 parte|adicional t|adts|temp.serviço|adic.temp.serv |sexta parte|temp. serviço|anueni|quadri|quiquenio|quinq|anuenio|anuênio|tempo ser|tempo de serviço|tmepo ser|tempo de srv|adicional por tempo de ser|adic. por tempo de seriço|adic. temp.serv.|adic. por tempo de seviço|adicionais por tempo de serv.|adic.por tempo de ser viços|anuencio|adicionais tempo srrviço|adicional de tempo de ser|adicional de tempo de servico|trienio|quiqnenio|trmpo serviço|adicional por tempo de servico|adic. por tempod e serviço|tmepo de serviço|tempos serviço|adc. tempo|de serviço inativo|adicionais por tempode serviço|tempo de  serviços|temp. de serviço|adicional de t. serviço|acréscimo por ano excedido 10%|adc. tem. sev.|quiqu|trênio|tempo de servicos|tempo de contribuição|triênio|adic. temp. de serviço|hora|6 parte|adicional t|adts|temp.serviço|adic.temp.serv |sexta parte|temp. serviço|anueni|quadri|quiquenio|quinq|anuenio|anuênio|tempo ser|tempo de serviço|tmepo ser|tempo de srv|ad. t.", df_proventos$col7, ignore.case = TRUE), "ATS", df_proventos$col7)
df_proventos$col7 <- ifelse(grepl("vent. pess|vant|vpni|v.pes|v. pes|vatnatagem|abono|diferença do pccr|a.pess|a.pes|vent. pess|vant|vpni|v.pes|v. pes|vatnatagem|abono|diferença do pccr|a.pess|a.pes|complement", df_proventos$col7, ignore.case = TRUE), "Vantagem Pessoal", df_proventos$col7)
df_proventos$col7 <- ifelse(grepl("pensão|provent|outros acresc. inatividade|provnto|aposent|prov|média|media|resultado|propor|rpovento|prvento|posent", df_proventos$col7, ignore.case = TRUE), "Proventos", df_proventos$col7)
df_proventos$col7 <- ifelse(grepl("sal. incorp.|vencimento|vecimento|piso nacional|venc.|vertical|dif pccr|diferença pccr|sal.incorp|venvi|salári|salari|remun|base|subsidio|subsídio|venecimento", df_proventos$col7, ignore.case = TRUE), "Vencimento", df_proventos$col7)
df_proventos$col7 <- ifelse(grepl("insalubri|isalu|insalub", df_proventos$col7, ignore.case = TRUE), "Insalubridade", df_proventos$col7)
df_proventos$col7 <- ifelse(grepl("antec", df_proventos$col7, ignore.case = TRUE), "Antecipação de Aumento", df_proventos$col7)
df_proventos$col7 <- ifelse(grepl("represen|rep.|represetação", df_proventos$col7, ignore.case = TRUE), "Representação", df_proventos$col7)
df_proventos$col7 <- ifelse(grepl("gifs|qualif|titul|especialização", df_proventos$col7, ignore.case = TRUE), "Adc. Qualificação", df_proventos$col7)
df_proventos$col7 <- ifelse(grepl("adicional|risco de vida|pó de giz|acrescimo 20|produção e prod.|outros acrescimos pecuniarios|periculosidade|adicionais de permanência|gead|adic. de permanencia|regencia de classe|serv. extra.|produção prod.|serviço extra incorporado|horas|outros acresc.pecuniarios|bolsa avaliacao desemp.docente|gal|assidui|grat|magistério|magisterio|vpnr|complemento|produtivid|gcex|gpcex|noturno|adic. not.|gae|jornada ampliada|bolsa de sempenho fiscal|efetivo desempenho|adi.jorn.ampl|risco de vida|pó de giz|acrescimo 20|produção e prod.|outros acrescimos pecuniarios|periculosidade|adicionais de permanência|gead|adic. de permanencia|regencia de classe|serv. extra.|produção prod.|serviço extra incorporado|horas|outros acresc.pecuniarios|bolsa avaliacao desemp.docente|gal|assidui|grat|magistério|magisterio|vpnr|complemento|produtivid|gcex|gpcex|noturno|adic. not.|gaj|g.a.p|gap|gati|gshu|gdp|grat|gal|noturno|vpnr|prod. s. bucal", df_proventos$col7, ignore.case = TRUE), "Gratificação", df_proventos$col7)
df_proventos$col7 <- ifelse(grepl("ajuda de custo", df_proventos$col7, ignore.case = TRUE), "Indenizatorio", df_proventos$col7)
df_proventos$col7 <- ifelse(grepl("^[0-9]|90", df_proventos$col7, ignore.case = TRUE), "0", df_proventos$col7)
# Definindo valores que não se enquadram nas categorias acima como "0"
df_proventos$col7 <- ifelse(!grepl("Proventos|Ind|Rep|ATS|Venc|Insa|Qualif|Antec|Vant|Grat", df_proventos$col7, ignore.case = TRUE), "0", df_proventos$col7)

# Movimento na coluna 8:

df_proventos$col8 <- ifelse(grepl("hora|6 parte|adicional t|adts|temp.serviço|adic.temp.serv |sexta parte|temp. serviço|anueni|quadri|quiquenio|quinq|anuenio|anuênio|tempo ser|tempo de serviço|tmepo ser|tempo de srv|adicional por tempo de ser|adic. por tempo de seriço|adic. temp.serv.|adic. por tempo de seviço|adicionais por tempo de serv.|adic.por tempo de ser viços|anuencio|adicionais tempo srrviço|adicional de tempo de ser|adicional de tempo de servico|trienio|quiqnenio|trmpo serviço|adicional por tempo de servico|adic. por tempod e serviço|tmepo de serviço|tempos serviço|adc. tempo|de serviço inativo|adicionais por tempode serviço|tempo de  serviços|temp. de serviço|adicional de t. serviço|acréscimo por ano excedido 10%|adc. tem. sev.|quiqu|trênio|tempo de servicos|tempo de contribuição|triênio|adic. temp. de serviço|hora|6 parte|adicional t|adts|temp.serviço|adic.temp.serv |sexta parte|temp. serviço|anueni|quadri|quiquenio|quinq|anuenio|anuênio|tempo ser|tempo de serviço|tmepo ser|tempo de srv|ad. t.", df_proventos$col8, ignore.case = TRUE), "ATS", df_proventos$col8)
df_proventos$col8 <- ifelse(grepl("vent. pess|vant|vpni|v.pes|v. pes|vatnatagem|abono|diferença do pccr|a.pess|a.pes|vent. pess|vant|vpni|v.pes|v. pes|vatnatagem|abono|diferença do pccr|a.pess|a.pes|complement", df_proventos$col8, ignore.case = TRUE), "Vantagem Pessoal", df_proventos$col8)
df_proventos$col8 <- ifelse(grepl("pensão|provent|outros acresc. inatividade|provnto|aposent|prov|média|media|resultado|propor|rpovento|prvento|posent", df_proventos$col8, ignore.case = TRUE), "Proventos", df_proventos$col8)
df_proventos$col8 <- ifelse(grepl("sal. incorp.|vencimento|vecimento|piso nacional|venc.|vertical|dif pccr|diferença pccr|sal.incorp|venvi|salári|salari|remun|base|subsidio|subsídio|venecimento", df_proventos$col8, ignore.case = TRUE), "Vencimento", df_proventos$col8)
df_proventos$col8 <- ifelse(grepl("insalubri|isalu|insalub", df_proventos$col8, ignore.case = TRUE), "Insalubridade", df_proventos$col8)
df_proventos$col8 <- ifelse(grepl("antec", df_proventos$col8, ignore.case = TRUE), "Antecipação de Aumento", df_proventos$col8)
df_proventos$col8 <- ifelse(grepl("represen|rep.|represetação", df_proventos$col8, ignore.case = TRUE), "Representação", df_proventos$col8)
df_proventos$col8 <- ifelse(grepl("gifs|qualif|titul|especialização", df_proventos$col8, ignore.case = TRUE), "Adc. Qualificação", df_proventos$col8)
df_proventos$col8 <- ifelse(grepl("adicional|risco de vida|pó de giz|acrescimo 20|produção e prod.|outros acrescimos pecuniarios|periculosidade|adicionais de permanência|gead|adic. de permanencia|regencia de classe|serv. extra.|produção prod.|serviço extra incorporado|horas|outros acresc.pecuniarios|bolsa avaliacao desemp.docente|gal|assidui|grat|magistério|magisterio|vpnr|complemento|produtivid|gcex|gpcex|noturno|adic. not.|gae|jornada ampliada|bolsa de sempenho fiscal|efetivo desempenho|adi.jorn.ampl|risco de vida|pó de giz|acrescimo 20|produção e prod.|outros acrescimos pecuniarios|periculosidade|adicionais de permanência|gead|adic. de permanencia|regencia de classe|serv. extra.|produção prod.|serviço extra incorporado|horas|outros acresc.pecuniarios|bolsa avaliacao desemp.docente|gal|assidui|grat|magistério|magisterio|vpnr|complemento|produtivid|gcex|gpcex|noturno|adic. not.|gaj|g.a.p|gap|gati|gshu|gdp|grat|gal|noturno|vpnr|prod. s. bucal", df_proventos$col8, ignore.case = TRUE), "Gratificação", df_proventos$col8)
df_proventos$col8 <- ifelse(grepl("ajuda de custo", df_proventos$col8, ignore.case = TRUE), "Indenizatorio", df_proventos$col8)
df_proventos$col8 <- ifelse(grepl("^[0-9]|90", df_proventos$col8, ignore.case = TRUE), "0", df_proventos$col8)
# Definindo valores que não se enquadram nas categorias acima como "0"
df_proventos$col8 <- ifelse(!grepl("Proventos|Ind|Rep|ATS|Venc|Insa|Qualif|Antec|Vant|Grat", df_proventos$col8, ignore.case = TRUE), "0", df_proventos$col8)

# Movimento na coluna 9:

df_proventos$col9 <- ifelse(grepl("hora|6 parte|adicional t|adts|temp.serviço|adic.temp.serv |sexta parte|temp. serviço|anueni|quadri|quiquenio|quinq|anuenio|anuênio|tempo ser|tempo de serviço|tmepo ser|tempo de srv|adicional por tempo de ser|adic. por tempo de seriço|adic. temp.serv.|adic. por tempo de seviço|adicionais por tempo de serv.|adic.por tempo de ser viços|anuencio|adicionais tempo srrviço|adicional de tempo de ser|adicional de tempo de servico|trienio|quiqnenio|trmpo serviço|adicional por tempo de servico|adic. por tempod e serviço|tmepo de serviço|tempos serviço|adc. tempo|de serviço inativo|adicionais por tempode serviço|tempo de  serviços|temp. de serviço|adicional de t. serviço|acréscimo por ano excedido 10%|adc. tem. sev.|quiqu|trênio|tempo de servicos|tempo de contribuição|triênio|adic. temp. de serviço|hora|6 parte|adicional t|adts|temp.serviço|adic.temp.serv |sexta parte|temp. serviço|anueni|quadri|quiquenio|quinq|anuenio|anuênio|tempo ser|tempo de serviço|tmepo ser|tempo de srv|ad. t.", df_proventos$col9, ignore.case = TRUE), "ATS", df_proventos$col9)
df_proventos$col9 <- ifelse(grepl("vent. pess|vant|vpni|v.pes|v. pes|vatnatagem|abono|diferença do pccr|a.pess|a.pes|vent. pess|vant|vpni|v.pes|v. pes|vatnatagem|abono|diferença do pccr|a.pess|a.pes|complement", df_proventos$col9, ignore.case = TRUE), "Vantagem Pessoal", df_proventos$col9)
df_proventos$col9 <- ifelse(grepl("pensão|provent|outros acresc. inatividade|provnto|aposent|prov|média|media|resultado|propor|rpovento|prvento|posent", df_proventos$col9, ignore.case = TRUE), "Proventos", df_proventos$col9)
df_proventos$col9 <- ifelse(grepl("sal. incorp.|vencimento|vecimento|piso nacional|venc.|vertical|dif pccr|diferença pccr|sal.incorp|venvi|salári|salari|remun|base|subsidio|subsídio|venecimento", df_proventos$col9, ignore.case = TRUE), "Vencimento", df_proventos$col9)
df_proventos$col9 <- ifelse(grepl("insalubri|isalu|insalub", df_proventos$col9, ignore.case = TRUE), "Insalubridade", df_proventos$col9)
df_proventos$col9 <- ifelse(grepl("antec", df_proventos$col9, ignore.case = TRUE), "Antecipação de Aumento", df_proventos$col9)
df_proventos$col9 <- ifelse(grepl("represen|rep.|represetação", df_proventos$col9, ignore.case = TRUE), "Representação", df_proventos$col9)
df_proventos$col9 <- ifelse(grepl("gifs|qualif|titul|especialização", df_proventos$col9, ignore.case = TRUE), "Adc. Qualificação", df_proventos$col9)
df_proventos$col9 <- ifelse(grepl("adicional|risco de vida|pó de giz|acrescimo 20|produção e prod.|outros acrescimos pecuniarios|periculosidade|adicionais de permanência|gead|adic. de permanencia|regencia de classe|serv. extra.|produção prod.|serviço extra incorporado|horas|outros acresc.pecuniarios|bolsa avaliacao desemp.docente|gal|assidui|grat|magistério|magisterio|vpnr|complemento|produtivid|gcex|gpcex|noturno|adic. not.|gae|jornada ampliada|bolsa de sempenho fiscal|efetivo desempenho|adi.jorn.ampl|risco de vida|pó de giz|acrescimo 20|produção e prod.|outros acrescimos pecuniarios|periculosidade|adicionais de permanência|gead|adic. de permanencia|regencia de classe|serv. extra.|produção prod.|serviço extra incorporado|horas|outros acresc.pecuniarios|bolsa avaliacao desemp.docente|gal|assidui|grat|magistério|magisterio|vpnr|complemento|produtivid|gcex|gpcex|noturno|adic. not.|gaj|g.a.p|gap|gati|gshu|gdp|grat|gal|noturno|vpnr|prod. s. bucal", df_proventos$col9, ignore.case = TRUE), "Gratificação", df_proventos$col9)
df_proventos$col9 <- ifelse(grepl("ajuda de custo", df_proventos$col9, ignore.case = TRUE), "Indenizatorio", df_proventos$col9)
df_proventos$col9 <- ifelse(grepl("^[0-9]|90", df_proventos$col9, ignore.case = TRUE), "0", df_proventos$col9)
# Definindo valores que não se enquadram nas categorias acima como "0"
df_proventos$col9 <- ifelse(!grepl("Proventos|Ind|Rep|ATS|Venc|Insa|Qualif|Antec|Vant|Grat", df_proventos$col9, ignore.case = TRUE), "0", df_proventos$col9)

# Movimento na coluna 10:

df_proventos$col10 <- ifelse(grepl("hora|6 parte|adicional t|adts|temp.serviço|adic.temp.serv |sexta parte|temp. serviço|anueni|quadri|quiquenio|quinq|anuenio|anuênio|tempo ser|tempo de serviço|tmepo ser|tempo de srv|adicional por tempo de ser|adic. por tempo de seriço|adic. temp.serv.|adic. por tempo de seviço|adicionais por tempo de serv.|adic.por tempo de ser viços|anuencio|adicionais tempo srrviço|adicional de tempo de ser|adicional de tempo de servico|trienio|quiqnenio|trmpo serviço|adicional por tempo de servico|adic. por tempod e serviço|tmepo de serviço|tempos serviço|adc. tempo|de serviço inativo|adicionais por tempode serviço|tempo de  serviços|temp. de serviço|adicional de t. serviço|acréscimo por ano excedido 10%|adc. tem. sev.|quiqu|trênio|tempo de servicos|tempo de contribuição|triênio|adic. temp. de serviço|hora|6 parte|adicional t|adts|temp.serviço|adic.temp.serv |sexta parte|temp. serviço|anueni|quadri|quiquenio|quinq|anuenio|anuênio|tempo ser|tempo de serviço|tmepo ser|tempo de srv|ad. t.", df_proventos$col10, ignore.case = TRUE), "ATS", df_proventos$col10)
df_proventos$col10 <- ifelse(grepl("vent. pess|vant|vpni|v.pes|v. pes|vatnatagem|abono|diferença do pccr|a.pess|a.pes|vent. pess|vant|vpni|v.pes|v. pes|vatnatagem|abono|diferença do pccr|a.pess|a.pes|complement", df_proventos$col10, ignore.case = TRUE), "Vantagem Pessoal", df_proventos$col10)
df_proventos$col10 <- ifelse(grepl("pensão|provent|outros acresc. inatividade|provnto|aposent|prov|média|media|resultado|propor|rpovento|prvento|posent", df_proventos$col10, ignore.case = TRUE), "Proventos", df_proventos$col10)
df_proventos$col10 <- ifelse(grepl("sal. incorp.|vencimento|vecimento|piso nacional|venc.|vertical|dif pccr|diferença pccr|sal.incorp|venvi|salári|salari|remun|base|subsidio|subsídio|venecimento", df_proventos$col10, ignore.case = TRUE), "Vencimento", df_proventos$col10)
df_proventos$col10 <- ifelse(grepl("insalubri|isalu|insalub", df_proventos$col10, ignore.case = TRUE), "Insalubridade", df_proventos$col10)
df_proventos$col10 <- ifelse(grepl("antec", df_proventos$col10, ignore.case = TRUE), "Antecipação de Aumento", df_proventos$col10)
df_proventos$col10 <- ifelse(grepl("represen|rep.|represetação", df_proventos$col10, ignore.case = TRUE), "Representação", df_proventos$col10)
df_proventos$col10 <- ifelse(grepl("gifs|qualif|titul|especialização", df_proventos$col10, ignore.case = TRUE), "Adc. Qualificação", df_proventos$col10)
df_proventos$col10 <- ifelse(grepl("adicional|risco de vida|pó de giz|acrescimo 20|produção e prod.|outros acrescimos pecuniarios|periculosidade|adicionais de permanência|gead|adic. de permanencia|regencia de classe|serv. extra.|produção prod.|serviço extra incorporado|horas|outros acresc.pecuniarios|bolsa avaliacao desemp.docente|gal|assidui|grat|magistério|magisterio|vpnr|complemento|produtivid|gcex|gpcex|noturno|adic. not.|gae|jornada ampliada|bolsa de sempenho fiscal|efetivo desempenho|adi.jorn.ampl|risco de vida|pó de giz|acrescimo 20|produção e prod.|outros acrescimos pecuniarios|periculosidade|adicionais de permanência|gead|adic. de permanencia|regencia de classe|serv. extra.|produção prod.|serviço extra incorporado|horas|outros acresc.pecuniarios|bolsa avaliacao desemp.docente|gal|assidui|grat|magistério|magisterio|vpnr|complemento|produtivid|gcex|gpcex|noturno|adic. not.|gaj|g.a.p|gap|gati|gshu|gdp", df_proventos$col10, ignore.case = TRUE), "Gratificação", df_proventos$col10)
df_proventos$col10 <- ifelse(grepl("ajuda de custo", df_proventos$col10, ignore.case = TRUE), "Indenizatorio", df_proventos$col10)
df_proventos$col10 <- ifelse(grepl("^[0-9]|90", df_proventos$col10, ignore.case = TRUE), "0", df_proventos$col10)
# Definindo valores que não se enquadram nas categorias acima como "0"
df_proventos$col10 <- ifelse(!grepl("Proventos|Ind|Rep|ATS|Venc|Insa|Qualif|Antec|Vant|Grat", df_proventos$col10, ignore.case = TRUE), "0", df_proventos$col10)


# Converter o dataframe para formato long
df_long <- df_proventos %>%
  pivot_longer(cols = starts_with("col"), names_to = "original_col", values_to = "value")

# Criar a coluna binária para cada valor distinto
df_wide <- df_long %>%
  distinct(protocolo, value) %>%
  mutate(value = paste0("parc_", value)) %>%
  pivot_wider(names_from = value, values_from = value, values_fill = 0, values_fn = list(value = ~1))

# Juntar de volta com o dataframe original
df_final <- df_proventos %>%
  left_join(df_wide, by = "protocolo")

df_final <- df_final %>%
  select(-parc_0)

colunas_para_remover <- c("col1", "col2", "col3", "col4", "col5", "col6", "col6", "col7", "col8", "col9", "col10")

# Supondo que df_final é o seu data frame final
df_final <- df_final %>%
  select(-one_of(colunas_para_remover))

# Substituir NA por 0
df_final[is.na(df_final)] <- 0

df_final <- df_final %>%
  rename(Sequencia = proventos)

# Realize o join entre os DataFrames usando a coluna 'protocolo' como chave de cruzamento
# E selecione as colunas desejadas do df_proventos usando a lista colunas_desejadas
dados_df <- merge(dados_df, df_final, by = "protocolo", all.x = TRUE)

dados_df <- dados_df %>% filter(protocolo != "06801/16")

library(writexl)
# Exportar para um arquivo Excel
# write_xlsx(dados_df, path = "Base_Conferencia_8c_R.xlsx")


colnames(dados_df)

# Selecionando e renomeando colunas
colunas_selecionadas <- c('protocolo', 'PARA_Jurisdicionado', 'exercicio', 'PARA_ORGAO_PADR', 'PARA_CARREIRA_PADR','PARA_cargo', 'PARA_aposentadoriaespecial', 'PARA_MEIOP_PADR', 'PARA_descricao','qtd_vinculos_padr', 'qtd_vinculos_pub_padr', 'qtd_vinculos_privados_padr','qtd_vinculos_mesmo_cargo_padr', 'qtd_vinculos_mesma_carreira_padr', 'parc_Vencimento','parc_ATS','parc_Proventos', 'parc_Gratificação', 'parc_Vantagem Pessoal', 'parc_Insalubridade', 'parc_Adc. Qualificação','parc_Representação', 'parc_Antecipação de Aumento', 'parc_Indenizatorio', 'indicador_sm', 'rotulo')

dados_df <- dados_df[, colunas_selecionadas]

# Dicionário de mapeamento para renomear as colunas
novo_nome_colunas <- c('nr_proc', 'jurisdicionado', 'ano', 'orgao', 'carreira', 'cargo', 'flag_aposesp', 'meio_publi', 'regra_apos','qtd_vinculos', 'qtd_vinculos_pub', 'qtd_vinculos_priv', 'qtd_vinculos_mesmo_cargo','qtd_vinculos_mesma_carreira', 'p_vencimento', 'p_ats', 'p_proventos', 'p_gratificacao','p_vantpessoal','p_insalubridade', 'p_adcqualificacao', 'p_representacao', 'p_antecipacao','p_indenizatorio', 'ind_sm', 'rotulo')

# Renomeando as colunas usando setNames
colnames(dados_df) <- setNames(novo_nome_colunas, colunas_selecionadas)


write_xlsx(dados_df, path = "Base_Conferencia_9_R.xlsx")
