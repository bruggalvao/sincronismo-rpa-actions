import psycopg2
import pandas as pd

def merge_tabela(nome_tabela_origem: str, nome_tabela_destino: str, conn_origem, conn_destino):
    # Criando cursores
    cursor_origem = conn_origem.cursor()
    cursor_destino = conn_destino.cursor()

    # Pegando os dados da tabela de origem
    df_origem = pd.read_sql(f'SELECT * FROM {nome_tabela_origem}', conn_origem)

    # Pegando a PK que vai ser usada da tabela origem
    sql_pk_origem = f"""
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name  = kcu.constraint_name
         AND tc.table_schema     = kcu.table_schema
        
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema    = 'public'
          AND tc.table_name      = '{nome_tabela_origem}';
    """

    cursor_origem.execute(sql_pk_origem)
    rows = cursor_origem.fetchone()
    pk_origem = rows[0]

    # Pegando todas a lista de todas as colunas da tabela origem
    sql_colunas_origem = f"""
        SELECT column_name, udt_name, is_nullable
          FROM information_schema.columns
         WHERE table_name = '{nome_tabela_origem}'
         ORDER BY ordinal_position;
    """

    cursor_origem.execute(sql_colunas_origem)
    rows_colunas_origem = cursor_origem.fetchall()
    colunas_origem = df_origem.columns

    # Pegando todas as colunas de destino
    sql_colunas_destino = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{nome_tabela_destino}'
        ORDER BY ordinal_position;
    """

    cursor_destino.execute(sql_colunas_destino)
    rows = cursor_destino.fetchall()
    colunas_destino = [r[0] for r in rows]

    # Comparando as colunas de origem com as colunas de destino
    novas_colunas = set(colunas_origem) - set(colunas_destino)
    
    defaults = {
    "integer": 1,
    "int4": 1,
    "bigint": 1,
    "numeric": 1,
    "boolean": "FALSE",
    "text": "''",
    "varchar": "''",
    "date": "CURRENT_DATE",
    "timestamp": "NOW()"
    }

    if novas_colunas:
        for row in rows_colunas_origem:
            ddl_query = f'ALTER TABLE {nome_tabela_destino} ADD COLUMN '
            if row[0] in novas_colunas:
                ddl_query += f'{row[0]} {row[1]}'

                # Caso seja um campo NOT NULL, adicionar default
                if row[2] == 'NO':
                    ddl_query += f"NOT NULL DEFAULT {defaults.get(row[1])}"
            cursor_destino.execute(ddl_query)    

    colunas_origem_sem_pk = [c for c in colunas_origem if c != pk_origem]

    # Values a serem utilizados
    placeholders = ", ".join(["%s"] * len(colunas_origem))

    # Lista de colunas
    cols_alias = ", ".join(colunas_origem)

    # Criando UPDATE SET statement
    update_set = ", ".join([f"{c} = v.{c}" for c in colunas_origem_sem_pk ])

    # Criando WHEN NOT MATCHED statement
    insert_cols = ", ".join(colunas_origem)
    insert_vals = ", ".join([f"v.{c}" for c in colunas_origem])

    sql_merge = f"""
    MERGE INTO {nome_tabela_destino} AS z
    USING (VALUES ({placeholders})) AS v({cols_alias})
    ON v.{pk_origem} = z.{pk_origem}
    WHEN MATCHED THEN
        UPDATE SET {update_set}
    WHEN NOT MATCHED THEN
        INSERT ({insert_cols}) VALUES ({insert_vals});
    """

    # executando o comando de merge a cada iteração do dataframe
    for row in df_origem.itertuples(index=False, name=None):
        cursor_destino.execute(sql_merge, row)
        conn_destino.commit()

    conn_origem.close()
    conn_destino.close()

try:
    conn_grupo2 = psycopg2.connect(
        host="pg-f312315-fellipe1-635.g.aivencloud.com",
        database="sincronizacao",
        user="avnadmin",
        password="AVNS_NQ8VPFAPwUB1XL3R4G5",
        port="28296"
    )

    print("Conexão bem-sucedida - GRUPO 2!")

except (Exception, psycopg2.Error) as error:
    print("Erro ao conectar com o banco do Grupo 2", error)

try:
    conn_grupo3 = psycopg2.connect(
        host="pg-grupo3-germinare-cb4e.b.aivencloud.com",
        database="sincronizacao",
        user="avnadmin",
        password="AVNS_aMUcK8LZcVhNYGqzkdh",
        port="14666"
    )
    
    print("Conexão bem-sucedida - GRUPO 3!")

except (Exception, psycopg2.Error) as error:
    print("Erro ao conectar com o banco do Grupo 3", error)

cursor_grupo2 = conn_grupo2.cursor()
cursor_grupo3 = conn_grupo3.cursor()

# sql_tabelas_origem = """
#     SELECT table_name
#     FROM information_schema.tables
#     WHERE table_schema = 'public'
#     AND table_type = 'BASE TABLE';
# """
# cursor_grupo2.execute(sql_tabelas_origem)
# rows = cursor_grupo2.fetchall()
# tabelas_origem = [r[0] for r in rows]

tabelas_origem = ['paises', 'fases', 'campeonatos', 'jogadores', 'partidas', 'chaveamento']

for tabela in tabelas_origem:
    merge_tabela(tabela, tabela, conn_grupo2, conn_grupo3)

del cursor_grupo2
del rows

