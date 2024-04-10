from modulos.compras import *

def almoxarif_para_ccusto(): # Converte cada almxoarifado como sendo um centro de custo
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

def requi_saldo_ant(): # Insere saldo anterior no estoque
    cur_fdb.execute("delete from icadreq where requi starting '000000/'")
    cur_fdb.execute("delete from requi where requi starting '000000/'")

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

def requi(): # Insere requisição do exercício
    cur_fdb.execute(f"delete from icadreq where id_requi in (select id_requi from requi where requi <> '000000/{ANO%2000}' and conversao <> 'S')")
    cur_fdb.execute(f"delete from requi where requi <> '000000/{ANO%2000}' and conversao <> 'S'")
    cria_campo('ALTER TABLE requi ADD nrodct_ant varchar(20)')
    
    insert_requi = cur_fdb.prep("""INSERT INTO requi (EMPRESA, ID_REQUI, requi, num, ano, destino, CODCCUSTO, DTLAN,
                            DATAE, ENTR, said, COMP, TIPOSAIDA, TPREQUI, obs, nrodct_ant) 
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""")
    
    insert_icadreq = cur_fdb.prep("""insert into icadreq (id_requi, requi, codccusto, empresa, item, quan1, vaun1, vato1, quan2, vaun2, vato2, cadpro, destino) 
                                values (?,?,?,?,?,?,?,?,?,?,?,?,?)""")

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
    num_max = int(cur_fdb.execute('select max(num) from requi').fetchone()[0])
    nrodct_ant = '00000000000000000000'
    
    for row in tqdm(consulta, desc='ESTOQUE - Inserindo Requisição do Exercício'):
        if (row['nrodct'] != nrodct_ant) or (row['nrodct'] == nrodct_ant and row['tipo'] != tipo_ant):
            empresa = EMPRESA
            id_requi += 1 
            num_max += 1
            requi = f'{str(num_max).zfill(6)}/{ANO%2000}'
            num = str(num_max).zfill(6)
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
        if row['tipo'] == 'E': # Se for entrada
            quan1 = abs(row['quan1'])
            vaun1 = row['vaun1']
            vato1 = abs(float(row['vato1']))
            quan2, vaun2, vato2 = 0, 0, 0
        else: # Se for saída
            quan1, vaun1, vato1 = 0, 0, 0
            quan2 = abs(row['quan1'])
            vaun2 = row['vaun1']
            vato2 = abs(float(row['vato1']))
        cadpro = PRODUTOS[row['cadpro']]
        cur_fdb.execute(insert_icadreq,(id_requi, requi, codccusto, empresa, item, quan1, vaun1, vato1, quan2, vaun2, vato2, cadpro, destino))
    commit()

def subpedidos(): # Insere BRM's como subpedidos
    cria_campo('alter table requi add conversao varchar(1)')
    cur_fdb.execute('''delete from icadreq where requi in (select requi from requi where conversao = 'S')''')
    cur_fdb.execute("delete from requi where conversao = 'S'")
    requi_ant = '00000000000000000000'

    consulta = fetchallmap(f"""select
                                    *,
                                    ROW_NUMBER() over (PARTITION by [requi]
                                order by
                                    [requi]) item
                                from
                                    (
                                    select
                                        right('000000'+cast(a.brmnum as varchar),6)+ '/' + SUBSTRING(cast(a.brmano as varchar),3,4) requi,
                                        right('000000'+cast(a.brmnum as varchar),6) num,
                                        a.brmano ano,
                                        brmdat dtlan,
                                        datrec datae,
                                        almox1 + almox2 + almox3 codccusto,
                                        right('000000000' +(almox1 + almox2 + almox3),
                                        9) destino,
                                        b.estrut + '.' + b.grupo + '.' + b.subgrp + '.' + b.itemat + '-' + b.digmat cadpro,
                                        case
                                            when qtde = 0 then 1
                                            else qtde
                                        end quan1,
                                        pcouni vaun1,
                                        totmer vato1,
                                        RIGHT('00000' + cast(a.af AS varchar),
                                        5)+ '/' + SUBSTRING(a.nafano, 3, 2) numped,
                                        'S' conversao,
                                        c.cnfnf
                                    from
                                        mat.MET68900 a
                                    join mat.MET69100 b
                                                on
                                        a.brmano = b.brmano
                                        and a.brmnum = b.brmnum
                                        and a.brmdig = b.brmdig
                                    left join mat.MXT60600 c on 
                                    a.af = c.af and a.brmano = c.brmano and a.brmnum = c.brmnum
                                    where
                                        a.nafano >= {ANO})query
                                order by
                                    [requi],
                                    [item]
                    """)

    id_requi = int(cur_fdb.execute('select coalesce(max(id_requi),0) from requi').fetchone()[0])

    insert_requi = cur_fdb.prep("""INSERT INTO requi (EMPRESA, ID_REQUI, requi, num, ano, destino, CODCCUSTO, DTLAN,
                            DATAE, ENTR, said, entr_said, COMP, TIPOSAIDA, TPREQUI, obs, conversao, numped, docum) 
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""")

    insert_icadreq = cur_fdb.prep("""insert into icadreq (id_requi, requi, codccusto, empresa, item, quan1, vaun1, vato1, quan2, vaun2, vato2, cadpro, destino) 
                                values (?,?,?,?,?,?,?,?,?,?,?,?,?)""")

    for row in tqdm(consulta, desc='PEDIDOS - Inserindo Subpedidos'):
        if row['requi'] != requi_ant:
            empresa = EMPRESA
            id_requi += 1 
            requi = row['requi']
            num = row['num']
            ano = row['ano']
            destino = row['destino']
            codccusto = row['codccusto']
            dtlan = row['dtlan']
            datae = row['datae']
            entr = 'S'
            said = 'S'
            entr_said = 'S'
            comp = 3
            tiposaida = 'P'
            tprequi = 'OUTRA'
            obs = f"REQUISIÇÃO - {row['requi']}"
            numped = row['numped']
            conversao = row['conversao']
            docum = row['cnfnf']
            requi_ant = row['requi']

            cur_fdb.execute(insert_requi,(empresa, id_requi, requi, num, ano, destino, codccusto, dtlan, datae, entr, said, entr_said, comp, tiposaida, tprequi, obs, conversao, numped, docum))

        item = row['item']
        quan1, quan2 = abs(row['quan1']), abs(row['quan1'])
        vaun1, vaun2 = row['vaun1'], row['vaun1']
        vato1, vato2 = abs(float(row['vato1'])), abs(float(row['vato1']))
        cadpro = PRODUTOS[row['cadpro']]
        cur_fdb.execute(insert_icadreq,(id_requi, requi, codccusto, empresa, item, quan1, vaun1, vato1, quan2, vaun2, vato2, cadpro, destino))
    commit()
    cur_fdb.execute("update requi a set a.id_cadped = (select b.id_cadped from cadped b where a.numped=b.numped")