import requests
from tinydb import TinyDB


def extrair():
    url = "https://api.coinbase.com/v2/prices/spot"
    response = requests.get(url)
    return response.json()

def transformar(dados):
    valor = dados['data']['amount']
    criptomeda = dados["data"]["base"]
    moeda = dados["data"]["currency"]
    
    dados_tratados = {
        "valor": valor,
        "criptomoeda": criptomeda,
        "moeda" : moeda
    }
    
    return dados_tratados

def load(dados_tratados):
    db = TinyDB('./db/db.json')
    db.insert(dados_tratados)
    print("Dados salvos com sucesso!")

if __name__ == "__main__":
    dados = transformar(extrair())
    load(dados)