from conexao import *
from ..tools import *
from tqdm import tqdm

PRODUTOS = produtos()

def almoxarif_para_ccusto():
    consulta = cur_fdb.execute("""select cod_ant, cod destino, cast(cod as integer) as codccusto, empresa, desti descr from destino""").fetchallmap()
    insert = cur_fdb.prep("""insert into centrocusto (poder, orgao, unidade, destino, ccusto, descr, empresa, codccusto, ocultar) values (?,?,?,?,?,?,?,?,?)""")

    for row in tqdm(consulta, desc='Definindo cada almoxarifado como pseudo-centro de custo'):
        poder_orgao_unidade = row['cod_ant'].split('.')
        
        poder = poder_orgao_unidade[0].zfill(2)
        orgao = poder_orgao_unidade[1].zfill(2)
        unidade = poder_orgao_unidade[2][1:]
        destino = row['destino']
        ccusto = '001'
        descr = f"ALMOXARIFADO - {row['descr']}"
        empresa = row['empresa']
        codccusto = row['codccusto']
        cur_fdb.execute(insert,(poder, orgao, unidade, destino, ccusto, descr, empresa, codccusto, 'N'))
    commit()

def requi_saldo_ant():
    cur_fdb.execute("delete from requi")
    cur_fdb.execute("delete from icadreq")

    consulta = fetchallmap(f"""select *, [vato1] / [quan1] vaun1 from (select 
                                1 id_requi,
                                '000000/{ANO%2000}' requi,
                                almox1+almox2+almox3 codccusto,
                                ROW_NUMBER() OVER (ORDER BY almox1, almox2, almox3, estrut, grupo, subgrp, itemat, digmat) AS item,
                                sum(case t.tipo_entsai when 'E' then qtdent else -qtdate  end ) quan1,
                                sum(case t.tipo_entsai when 'E' then totite else -totite end ) vato1,
                                estrut + '.' + grupo + '.' + subgrp + '.' + itemat + '-' + digmat cadpro,
                                right('000000000'+(almox1+almox2+almox3),9) destino
                            From mat.MET70100  e 
                            join mat.met91600 t on t.cmatip  = e.cmatip 
                            where year(dtadct) < {ANO}
                            group by almox1, almox2, almox3,estrut,grupo,subgrp,itemat,digmat
                            /*estrut  = 1
                            and grupo  = '03'
                            and subgrp = '10'
                            and itemat  = '0241'
                            and digmat = 0
                            and almox1 = 4
                            and almox2 = '01'
                            --and cancelado_brm  = 0
                            --and t.tipo_entsai = 'S'*/) as query
                            where [quan1] <> 0""")

    insert_requi = cur_fdb.prep("""INSERT INTO requi (EMPRESA, ID_REQUI, requi, num, ano, destino, CODCCUSTO, DTLAN,
                            DATAE, ENTR, said, COMP, TIPOSAIDA, TPREQUI, obs) 
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""")
    
    insert_icadreq = cur_fdb.prep("""insert into icadreq (id_requi, requi, codccusto, empresa, item, quan1, vaun1, vato1, cadpro, destino) 
                                values (?,?,?,?,?,?,?,?,?,?)""")
    
    try:
        cur_fdb.execute(insert_requi,(EMPRESA, 1, f'000000/{ANO%2000}','000000',ANO, '000901001', '901001', '2024-01-01', '2024-01-01', 'S', 'S', 'P', 'P', 'OUTRA', 'SALDO ANTERIOR'))
    except:
        pass

    for row in tqdm(consulta, desc='ESTOQUE - Inserindo Saldo Anterior'):
        id_requi = row['id_requi']
        requi = row['requi']
        codccusto = row['codccusto']
        empresa = EMPRESA
        item = row['item']
        quan1 = row['quan1']
        vaun1 = row['vaun1']
        vato1 = row['vato1']
        cadpro = PRODUTOS[row['cadpro']]
        destino = row['destino']
        cur_fdb.execute(insert_icadreq,(id_requi, requi, codccusto, empresa, item, quan1, vaun1, vato1, cadpro, destino))
    commit()

def requi():
    cur_fdb.execute(f"delete from icadreq where requi <> '000000/{ANO%2000}'")
    cur_fdb.execute(f"delete from requi where requi <> '000000/{ANO%2000}'")
    cria_campo('ALTER TABLE requi ADD nrodct_ant varchar(20)')
    
    insert_requi = cur_fdb.prep("""INSERT INTO requi (EMPRESA, ID_REQUI, requi, num, ano, destino, CODCCUSTO, DTLAN,
                            DATAE, ENTR, said, COMP, TIPOSAIDA, TPREQUI, obs, nrodct_ant) 
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""")
    
    insert_icadreq = cur_fdb.prep("""insert into icadreq (id_requi, requi, codccusto, empresa, item, quan1, vaun1, vato1, cadpro, destino) 
                                values (?,?,?,?,?,?,?,?,?,?)""")

    consulta = fetchallmap(f"""SELECT
                                    *,
                                    [vato1] / [quan1] AS vaun1,
                                    CASE
                                        WHEN [quan1] < 0 THEN 'S'
                                        ELSE 'E'
                                    END AS tipo,
                                    ROW_NUMBER() OVER (PARTITION BY nrodct, CASE WHEN [quan1] < 0 THEN 'S' ELSE 'E' END ORDER BY nrodct) AS itens
                                FROM
                                    (
                                    SELECT
                                        nrodct,
                                        dtadct,
                                        almox1 + almox2 + almox3 AS codccusto,
                                        SUM(CASE t.tipo_entsai WHEN 'E' THEN qtdent ELSE -qtdate END) AS quan1,
                                        SUM(CASE t.tipo_entsai WHEN 'E' THEN totite ELSE -totite END) AS vato1,
                                        estrut + '.' + grupo + '.' + subgrp + '.' + itemat + '-' + digmat AS cadpro,
                                        RIGHT('000000000' + (almox1 + almox2 + almox3), 9) AS destino
                                    FROM
                                        mat.MET70100 e
                                    JOIN mat.met91600 t ON t.cmatip = e.cmatip
                                    WHERE
                                        YEAR(dtadct) = {ANO}
                                    GROUP BY
                                        nrodct,
                                        dtadct,
                                        almox1,
                                        almox2,
                                        almox3,
                                        itens,
                                        estrut,
                                        grupo,
                                        subgrp,
                                        itemat,
                                        digmat
                                    ) AS query
                                WHERE
                                    [quan1] <> 0
                                ORDER BY
                                    nrodct,
                                    itens;""")
    
    id_requi = int(cur_fdb.execute('select max(id_requi) from requi').fetchone()[0])
    nrodct_ant = '00000000000000000000'
    
    for row in tqdm(consulta, desc='ESTOQUE - Inserindo Requisição do Exercício'):
        if (row['nrodct'] != nrodct_ant) or (row['nrodct'] == nrodct_ant and row['tipo'] != tipo_ant):
            empresa = EMPRESA
            id_requi += 1 
            requi = f'{str(id_requi).zfill(6)}/{ANO%2000}'
            num = str(id_requi).zfill(6)
            ano = ANO
            destino = row['destino']
            codccusto = row['codccusto']
            dtlan = row['dtadct']
            datae = row['dtadct']
            if row['tipo'] == 'E':
                entr = 'S'
                said = 'N'
            else: 
                entr = 'N'
                said = 'S'
            comp = 3
            tiposaida = 'P'
            tprequi = 'OUTRA'
            obs = f"REQUISIÇÃO - {row['nrodct']}"
            nrodct_ant = row['nrodct']
            tipo_ant = row['tipo']
            cur_fdb.execute(insert_requi,(empresa, id_requi, requi, num, ano, destino, codccusto, dtlan, datae, entr, said, comp, tiposaida, tprequi, obs, nrodct_ant))

        item = row['itens']
        quan1 = abs(row['quan1'])
        vaun1 = row['vaun1']
        vato1 = abs(float(row['vato1']))
        cadpro = PRODUTOS[row['cadpro']]
        cur_fdb.execute(insert_icadreq,(id_requi, requi, codccusto, empresa, item, quan1, vaun1, vato1, cadpro, destino))
    commit()