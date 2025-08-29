import requests
from tinydb import TinyDB
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
from time import sleep
import os
from dotenv import load_dotenv


load_dotenv()


DB_BASE = os.getenv('DB_BASE')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
API_URL = os.getenv('API_URL')



DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_BASE}"

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()

class BitcoinDados(Base):
    __tablename__ = "bitcoin_dados"

    id = Column(Integer, primary_key=True)
    valor = Column(Float)
    criptomoeda = Column(String(10))
    moeda = Column(String(10))
    timestamp = Column(DateTime)

def extrair():
    url = API_URL
    response = requests.get(url)
    return response.json()

def transformar(dados):
    valor = float(dados['data']['amount'])
    criptomoeda = dados["data"]["base"]
    moeda = dados["data"]["currency"]
    
    
    dados_tratados = BitcoinDados(
        valor=valor,
        criptomoeda=criptomoeda,
        moeda=moeda,
        timestamp=datetime.now(),  # não precisa formatar, deixa como datetime
    )

    return dados_tratados

def salvar_dados_sqlalchemy(dados):
    """Salva os dados no PostgreSQL usando SQLAlchemy."""
    with Session() as session:
        session.add(dados)
        session.commit()
        print("Dados salvos no PostgreSQL!")

def load(dados_tratados):
    db = TinyDB("./db/db.json")
    db.insert(
        {
            "valor": dados_tratados.valor,
            "criptomoeda": dados_tratados.criptomoeda,
            "moeda": dados_tratados.moeda,
            "timestamp": dados_tratados.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        }
    )
    print("Dados salvos no TinyDB!")


if __name__ == "__main__":
    
    Base.metadata.create_all(engine)

    while True:
        dados = transformar(extrair())
        load(dados)
        salvar_dados_sqlalchemy(dados)
        sleep(15)  