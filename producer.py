from faker import Faker
from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

fake = Faker('pt_BR')
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

produtos_disponiveis = [
    {"nome": "Arroz 5kg", "preco_unitario": 19.99},
    {"nome": "Feijão 1kg", "preco_unitario": 7.99},
    {"nome": "Macarrão 500g", "preco_unitario": 4.99},
    {"nome": "Açúcar 1kg", "preco_unitario": 3.49},
    {"nome": "Café 500g", "preco_unitario": 14.99},
    {"nome": "Óleo de soja 900ml", "preco_unitario": 5.99},
    {"nome": "Farinha de trigo 1kg", "preco_unitario": 4.49},
    {"nome": "Leite 1L", "preco_unitario": 3.29},
    {"nome": "Margarina 500g", "preco_unitario": 2.99},
    {"nome": "Queijo Mussarela 400g", "preco_unitario": 13.99},
    {"nome": "Presunto 300g", "preco_unitario": 8.99},
    {"nome": "Salsicha 500g", "preco_unitario": 6.99},
    {"nome": "Tomate 1kg", "preco_unitario": 5.49},
    {"nome": "Cebola 1kg", "preco_unitario": 4.29},
    {"nome": "Alho 200g", "preco_unitario": 6.49},
    {"nome": "Laranja 1kg", "preco_unitario": 4.99},
    {"nome": "Maçã 1kg", "preco_unitario": 7.49},
    {"nome": "Banana 1kg", "preco_unitario": 3.99},
    {"nome": "Batata 5kg", "preco_unitario": 12.99},
    {"nome": "Cenoura 1kg", "preco_unitario": 4.99},
]

def gerar_mensagem_venda():
    data_venda = fake.date_time_between(start_date=datetime(2024, 1, 1), end_date='now')
    produtos_comprados = [
        {
            "nome_produto": produto["nome"],
            "quantidade": random.randint(1, 5),
            "preco_unitario": produto["preco_unitario"]
        }
        for produto in random.choices(produtos_disponiveis, k=random.randint(1, 5))
    ]
    return {
        "id_ordem": fake.uuid4(),
        "documento_cliente": fake.cpf(),
        "produtos_comprados": produtos_comprados,
        "data_hora_venda": data_venda.strftime("%d/%m/%Y %H:%M:%S")
    }

try:
    for _ in range(20):
        mensagem = gerar_mensagem_venda()
        producer.send('vendas_ecommerce', value=mensagem)
        print(f"Mensagem enviada: {mensagem}")
        time.sleep(3)
    
    producer.flush()

except KeyboardInterrupt:
    print("Produtor interrompido.")
except Exception as e:
    print(f"Erro: {e}")
finally:
    producer.close()
    print("Produtor finalizado.")

