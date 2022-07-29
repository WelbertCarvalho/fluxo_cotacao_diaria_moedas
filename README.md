
# fluxo_cotacao_diaria_moedas
Neste repositório estão os arquivos de um projeto de conexão com API de cotação de moedas e inserção em banco de dados MySQL.

Para que seja possível executar o código é necessário que se tenha uma instância de um banco MySQL e que se crie uma tabela chamada cotacao_moedas.

#### cotacao_moedas.sql
Contém o script para a criação da tabela onde os dados serão inseridos.

#### CotacaoDiaria.py
Este script realiza a conexão com uma API que retorna a cotação diária de moedas à sua escolha, converte os formatos de alguns dados e realiza a inserção m uma tabela criada préviamente.

#### ManipDadosMysql.py
Este script contém uma classe para realizar a conexão com um banco MySql e contém também algumas funções customizadas para inserir dados via cursor em uma tabela do banco.
