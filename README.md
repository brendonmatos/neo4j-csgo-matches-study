# CSGO Matches Analysis with Neo4j

Comparando performance do Neo4j com o MySQL

## Rodando o projeto

1. Baixe a base de dados [aqui](https://www.kaggle.com/datasets/mateusdmachado/csgo-professional-matches) e coloque na pasta

2. Instale as dependências

```bash
pip install -r requirements.txt
```

3. Subindo os bancos de dados

```bash
docker-compose up -d
```

4. Execute os scripts de inserção de dados

```bash
python insertMysql.py
python insertNeo4j.py
```

5. Acesse os bancos de dados

Ambiente interativo do Neo4j: [http://localhost:7474/browser/](http://localhost:7474/browser/)
MySQL: localhost:3306 //TODO