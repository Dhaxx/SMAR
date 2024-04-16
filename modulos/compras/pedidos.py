from modulos.compras import *

def cabecalho():
    cria_campo('ALTER TABLE cadped ADD af_ant varchar(10)')
    cria_campo('ALTER TABLE cadped ADD nafano_ant varchar(10)')
    cria_campo('ALTER TABLE cadped ADD codgrupo_ant varchar(10)')
    cria_campo('ALTER TABLE cadped ADD anogrupo_ant varchar(10)')
    cria_campo('ALTER TABLE cadped ADD numint_ant varchar(10)')
    cria_campo('ALTER TABLE cadped ADD anoint_ant varchar(10)')
    cria_campo('ALTER TABLE cadped ADD mascmod_ant varchar(100)')
    cria_campo('ALTER TABLE cadped ADD autorizacao varchar(1)')

    cur_fdb.execute('delete from icadped')
    cur_fdb.execute('delete from cadped')

    insert = cur_fdb.prep("""insert into cadped (numped, num, ano, datped, codif, total, entrou, codccusto, id_cadped, 
                                                 empresa, numlic, af_ant, nafano_ant, codgrupo_ant, anogrupo_ant, numint_ant, mascmod_ant, anoint_ant, autorizacao, nempg)
                         values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""")
    
    consulta = fetchallmap(f"""select
                                    distinct
                                    RIGHT('00000' + isnull(cast(a.af AS varchar), a.id),
                                    5)+ '/' + SUBSTRING(isnull(a.nafano, a.anoint), 3, 2) numped,
                                    RIGHT('00000' + isnull(cast(a.af AS varchar), a.id),
                                    5) num,
                                    isnull(a.nafano, a.anoint) ano,
                                    a.nafdta datped,
                                    a.codfor,
                                    rtrim(e.dcto01) insmf,
                                    'N' entrou,
                                    isnull(c.UnidOrc,
                                    '001.001.001.001.000') UnidOrc,
                                    a.id,
                                    cast(a.sigla as varchar)+ '-' + cast(a.convit as varchar)+ '/' + cast(a.anoc as varchar) mascmod,
                                    a.sigla,
                                    cast(a.convit as integer) convit,
                                    a.anoc,
                                    a.af af_ant,
                                    a.nafano nafano_ant,
                                    a.codgrupo codgrupo_ant,
                                    a.anogrupo anogrupo_ant,
                                    a.numint numint_ant,
                                    a.anoint anoint_ant,
                                    cast(f.empenho as integer) nempg,
                                    'N' autorizacao
                                from
                                    mat.MCT67000 a
                                join mat.MCT66800 b on
                                    a.numint = b.numint
                                    and a.anoint = b.anoint
                                left join mat.UnidOrcamentariaW c on
                                    a.idNivel5 = c.idNivel5
                                join mat.MXT60100 d on
                                    d.codfor = a.codfor
                                left join mat.MXT61400 e on
                                    d.codnom = e.codnom
                                left join mat.MCT80100 f on
                                    f.anoint = a.anoint
                                    and a.numint = f.numint
                                where
                                    a.anoint >= {ANO-5} and a.af is null
                                UNION all
                                    select
                                    distinct
                                    RIGHT('00000' + cast(a.af AS varchar),
                                    5)+ '/' + SUBSTRING(a.nafano, 3, 2) numped,
                                    RIGHT('00000' + cast(a.af as varchar),
                                    5) num,
                                    a.nafano ano,
                                    a.nafdta datped,
                                    a.codfor,
                                    rtrim(e.dcto01) insmf,
                                    'N' entrou,
                                    isnull(c.UnidOrc,
                                    '001.001.001.001.000') UnidOrc,
                                    a.id,
                                    cast(a.sigla as varchar)+ '-' + cast(a.convit as varchar)+ '/' + cast(a.anoc as varchar) mascmod,
                                    a.sigla,
                                    cast(a.convit as integer) convit,
                                    a.anoc,
                                    a.af af_ant,
                                    a.nafano nafano_ant,
                                    a.codgrupo codgrupo_ant,
                                    a.anogrupo anogrupo_ant,
                                    a.numint numint_ant,
                                    a.anoint anoint_ant,
                                    f.nbloq nempg,
                                    'S'
                                from
                                    mat.MCT67000 a
                                join mat.MCT66800 b on
                                    a.af = b.af
                                    and a.nafano = b.nafano
                                left join mat.UnidOrcamentariaW c on
                                    a.idNivel5 = c.idNivel5
                                join mat.MXT60100 d on
                                    d.codfor = a.codfor
                                left join mat.MXT61400 e on
                                    d.codnom = e.codnom
                                left join mat.MCT71200 f on
                                    f.af = a.af
                                    and a.nafano = f.anoaf
                                where
                                    a.nafano >= {ANO-5}
                                order by
                                    [num],
                                    [ano]""")
    
    for row in tqdm(consulta, desc='Pedidos - Cabecalho'):
        numped = row['numped']
        num = row['num']
        ano = row['ano']
        datped = row['datped'] if row['datped'] else '2024-01-01'
        codif = INSMF_FORNECEDOR.get(row['insmf'], row['codfor']) 
        total = '0'
        entrou = row['entrou']
        codccusto = CENTROCUSTOS.get(row['UnidOrc'], '0')
        id_cadped = row['id']
        empresa = EMPRESA
        af_ant = row['af_ant']
        nafano_ant = row['nafano_ant']
        codgrupo_ant = row['codgrupo_ant']
        anogrupo_ant = row['anogrupo_ant']
        numint_ant = row['numint_ant']
        mascmod_ant = row['mascmod']
        anoint_ant = row['anoint_ant']
        autorizacao = row['autorizacao']
        nempg = row['nempg']
        try:
            numlic = LICITACAO[(row['convit'],row['sigla'],row['anoc'])]
            cur_fdb.execute(insert,(numped, num, ano, datped, codif, total, entrou, codccusto, id_cadped, 
                                    empresa, numlic, af_ant, nafano_ant, codgrupo_ant, anogrupo_ant, numint_ant, mascmod_ant, anoint_ant, autorizacao, nempg))
        except Exception as e:
            print(e)
            print(f'Erro: {numped}\n')
    commit()

    sql_query = """UPDATE despes a SET a.NUMPED = (SELECT b.numped FROM cadped b WHERE a.anoint = b.anoint_ant AND a.numint = b.numint_ant), a.id_cadped = (SELECT c.id_cadped FROM cadped c WHERE a.anoint = c.anoint_ant AND a.numint = c.numint_ant) WHERE NUMINT IS NOT NULL"""
    sql_query = sql_query.replace('\xa0', ' ')
    cur_fdb.execute(sql_query)
    commit()

def itens():
    cur_fdb.execute('delete from icadped')
    cria_campo('ALTER TABLE icadped ADD af_ant varchar(10)')
    cria_campo('ALTER TABLE icadped ADD nafano_ant varchar(10)')

    insert = cur_fdb.prep('insert into icadped (numped, item, cadpro, qtd, prcunt, prctot, codccusto, id_cadped, af_ant, nafano_ant) values (?,?,?,?,?,?,?,?,?,?)')

    consulta = fetchallmap(f"""SELECT
                                RIGHT('00000' + CAST(a.id AS VARCHAR),
                                5) + '/' + SUBSTRING(a.anoint, 3, 2) AS numped,
                                isnull(b.nuitem, ROW_NUMBER() over (PARTITION by a.id ORDER by a.id)) nuitem,
                                b.estrut + '.' + b.grupo + '.' + b.subgrp + '.' + b.itemat + '-' + b.digmat AS cadpro,
                                b.qtde,
                                b.preco,
                                b.total,
                                ISNULL(c.UnidOrc,
                                '001.001.001.001.000') AS UnidOrc,
                                a.id,
                                a.af,
	                            a.nafano
                            FROM
                                mat.MCT67000 a
                            JOIN mat.MCT66800 b ON
                                a.numint = b.numint
                                and a.anoint = b.anoint
                            LEFT JOIN mat.UnidOrcamentariaW c ON
                                a.idNivel5 = c.idNivel5
                            WHERE
                                a.anoint >= {ANO-5}
                                and a.af is NULL
                            UNION ALL 
                            SELECT
                                RIGHT('00000' + CAST(a.af AS VARCHAR),
                                5) + '/' + SUBSTRING(a.nafano, 3, 2) AS numped,
                                isnull(b.nuitem, ROW_NUMBER() over (PARTITION by a.id ORDER by a.id)),
                                b.estrut + '.' + b.grupo + '.' + b.subgrp + '.' + b.itemat + '-' + b.digmat AS cadpro,
                                b.qtde,
                                b.preco,
                                b.total,
                                ISNULL(c.UnidOrc,
                                '001.001.001.001.000') AS UnidOrc,
                                a.id,
                                a.af,
                                a.nafano
                            FROM
                                mat.MCT67000 a
                            JOIN mat.MCT66800 b ON
                                a.af = b.af
                                and a.nafano = b.nafano
                            LEFT JOIN mat.UnidOrcamentariaW c ON
                                a.idNivel5 = c.idNivel5
                            WHERE
                                a.nafano >= {ANO-5}
                                and a.af is not NULL
                            order by
                                [numped],
                                [nuitem]""")
    
    for row in tqdm(consulta, desc='Pedidos - Cadastrando Itens'):
        numped = row['numped']
        item = row['nuitem']
        cadpro = PRODUTOS[row['cadpro']]
        qtd = row['qtde']
        prcunt = row['preco']
        prctot = row['total']
        codccusto = CENTROCUSTOS.get(row['UnidOrc'], '0')
        id_cadped = row['id']
        af_ant = row['af']
        nafano_ant = row['nafano']

        try:
            cur_fdb.execute(insert,(numped, item, cadpro, qtd, prcunt, prctot, codccusto, id_cadped, af_ant, nafano_ant))

            with open('Scripts/icadped.txt', 'a') as f:
                f.write(f"""insert into icadped (numped, item, cadpro, qtd, prcunt, prctot, codccusto, id_cadped, af_ant, nafano_ant) 
                            values ({numped, item, cadpro, qtd, prcunt, prctot, codccusto, id_cadped, af_ant, nafano_ant});\n""")
        except:
            continue
    commit()