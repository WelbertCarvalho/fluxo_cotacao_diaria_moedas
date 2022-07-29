import mysql.connector
from mysql.connector import Error
import pandas as pd
from datetime import datetime

class ManipDadosMysql:
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password


    def conectar_mysql(self):
        try:
            con = mysql.connector.connect(
                host = self.host,
                database = self.database,
                user = self.user,
                password = self.password
            )
            return con
        except:
            print(f'Não foi possível se conectar à base {self.database}. Verifique a conexão.')
            return None


    def insere_dados_com_arq_sql(self, arquivo, tabela, dados_json):
            arg_insert = tuple(dados_json.values())
            data = datetime.strptime(arg_insert[-1],'%Y-%m-%d %H:%M:%S').date()
            arq_sql = open('/home/welbert/Documents/fluxo_cotacao_diaria_moedas/sql/'+arquivo,'r')
            sql = arq_sql.read()
            arq_sql.close()
            con = self.conectar_mysql()
            cursor = con.cursor()
            permitir_entrada = self.verifica_entrada_duplic(tabela, dados_json)
            
            if permitir_entrada:
                cursor.execute(sql, arg_insert)
                print(f'O registro com data {data} foi inserido com sucesso no {self.database}.')
            else:
                print(f'O registro com data {data} não pôde ser inserido no {self.database}. Pois já consta na tabela.')

            cursor.close()
            con.commit()
            con.close()


    # Recebe uma string com o nome de uma tabela do banco de dados conectado (utilizada internamente em outras funções)
    # Retorna um objeto series do pandas com as datas a serem verificadas.
    def retorna_chaves_tabela(self, tabela):
        dados = self.copia_dados(tabela)
        dados = pd.DataFrame(dados)
        if dados.shape[0] == 0:
            dados = pd.Series({'Status':'permitir_entrada'})
        else:
            dados = pd.to_datetime(dados.iloc[:,-1]).dt.date
                
        return dados


    # Recebe o nome da tabela e um dicionário ou uma tupla cuja última chave é um datetime.
    # Retorna uma variável boleana.
    def verifica_entrada_duplic(self, tabela, dados_a_conferir):
            chaves_tabela = self.retorna_chaves_tabela(tabela)

            if type(dados_a_conferir) is not tuple:
                dados_a_conferir = tuple(dados_a_conferir.values())
            try:
                chave_a_conferir = datetime.strptime(dados_a_conferir[-1],'%Y-%m-%d %H:%M:%S').date()
            except:
                chave_a_conferir = dados_a_conferir[-1].date()

            if chaves_tabela.any() == 'permitir_entrada':
                permitir_entrada = True
            else:
                permitir_entrada = chave_a_conferir not in chaves_tabela.values

            return permitir_entrada


    # Recebe uma string com o nome de uma tabela do banco de dados conectado.
    # Retorna uma lista com tuplas contendo os dados.
    def copia_dados(self, tabela):        
        try:
            con = self.conectar_mysql()
            sql_select = f"select * from {tabela}"
            cursor = con.cursor()
            cursor.execute(sql_select)
            dados_origem = cursor.fetchall()
            cursor.close()
            con.close()
            return dados_origem
        except:
            return None
            

    ###### Verificar para inserir dados no DW

    # Recebe uma tabela e uma lista com tuplas contendo os dados a serem inseridos.
    # Retorna None. Neste caso a função realiza a inserção de dados no banco de dados escolhido.  
    def cola_dados(self, tabela, dados_origem):
        try:
            con = self.conectar_mysql()
            qtd_campos = f"{'%s,' * len(dados_origem[0])}"
            sql_insert = f"insert into {tabela} values ({qtd_campos[:-1]})"
            cursor = con.cursor()
            for dado in dados_origem:
                permitir_entrada = self.verifica_entrada_duplic(tabela, dado)
                if permitir_entrada:
                    cursor.execute(sql_insert, dado)
                    print(f'O registro com data {dado[-1].date()} foi inserido com sucesso no {self.database}.')                   
                else:
                    print(f'O registro com data {dado[-1].date()} não pôde ser inserido no {self.database}. Pois já consta na tabela.')
            cursor.close()
            con.commit()
            con.close()
        except:
            print('Não foi possível inserir os dados. Verifique os parâmetros informados.')
            return None
        



