from datetime import datetime
import requests
import json
from ManipDadosMysql import ManipDadosMysql

r = requests.get('https://economia.awesomeapi.com.br/json/daily/USD-BRL/0')
dados_json = json.loads(r.text)
dados_json = dict(dados_json[0])
chaves_float = ['high', 'low', 'varBid', 'pctChange', 'bid', 'ask']

for campo in dados_json:
    if campo in chaves_float:
        dados_json[campo] = float(dados_json[campo])


datalake = ManipDadosMysql(
    host='localhost', 
    database='datalake', 
    user='usuario', 
    password='senha'
)

data_warehouse = ManipDadosMysql(
    host='localhost',
    database='data_warehouse',
    user='usuario',
    password='senha'
)

# Para coletar os dados da API e inserir no Datalake
datalake.insere_dados_com_arq_sql('insere_dados_cotacao_moedas_dl.sql','cotacao_moedas', dados_json)

# Para copiar os dados do Datalake e inserir no DW
tab_cotacao_moedas_dl = datalake.copia_dados('cotacao_moedas')
data_warehouse.cola_dados('cotacao_moedas', tab_cotacao_moedas_dl)






