from modulos.compras import *

def cadastro():
    cur_fdb.execute("delete from icadorc")
    cur_fdb.execute("delete from cadorc")

    insert_cadorc = cur_fdb.prep("""insert
                                into
                                cadorc (id_cadorc,
                                num,
                                ano,
                                numorc,    
                                dtorc,
                                descr,  
                                prioridade,
                                obs,
                                status,
                                liberado,
                                codccusto,
                                liberado_tela,
                                empresa,
                                registropreco) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""")

    insert_icadorc = cur_fdb.prep(
        'insert into icadorc (numorc, item, cadpro, qtd, valor, itemorc, codccusto, itemorc_ag, id_cadorc) values (?,'
        '?,?,?,?,?,?,?,?)')

    consulta = fetchallmap("""
    SELECT
        cabecalho.numreq numero,
        cabecalho.anoreq ano,
        datreq dtorc,
        motdev descr,
        'NORMAL' prioridade,
        'Processo de Compra: ' +  cast(cabecalho.numreq as varchar) + '/' + cast(cabecalho.anoreq as varchar) obs,
        'AP' status,
        'S' liberado,
        cabecalho.idnivel5 codccusto,
        'L' liberado_tela,
        'N' registropreco,
        [item] = item.nuitem,
        [Requisitante] = uo.idNivel5,
        [cadpro] = produto.estrut + '.' + produto.grupo + '.' + produto.subgrp + '.' + produto.itemat + '-' + produto.digmat,
        [Quantidade] = item.quatde,
        [Valor Unitário] = item.valite,
        [Total item] = item.totite
    FROM
        mat.MCT63400 cabecalho
    JOIN mat.MCT63500 item ON
        item.numreq = cabecalho.numreq
        AND item.anoreq = cabecalho.anoreq
    JOIN mat.unidorcamentariaW uo on
        uo.idnivel5 = cabecalho.idnivel5
    JOIN mat.MXT62300 produto ON
        produto.estrut = item.estrut
        AND produto.grupo = item.grupo
        AND produto.subgrp = item.subgrp
        AND produto.itemat = item.itemat
        AND produto.digmat = item.digmat
    where
        cabecalho.anoreq >= %d
        and not exists(
        select
            1
        from
            mat.MCT79900 cotacao
        where
            cotacao.numreq = cabecalho.numreq
            and cotacao.anoreq = cabecalho.anoreq
            and cotacao.origem = 'C'
                                        )
    union all                                   
    select
        cabecalho.numreg num,
        cabecalho.anoreg ano,
        datreg dtorc,
        motdev descr,
        'NORMAL' prioridade,
        'Registro de Preço: ' +  cast(cabecalho.numreg as varchar) + '/' + cast(cabecalho.anoreg as varchar) obs,
        'AP' status,
        'S' liberado,
        cabecalho.idnivel5 codccusto,
        'L' liberado_tela,
        'S' registropreco,
        [item] = item.nuitem,
        [Requisitante] = uo.idNivel5,
        [cadpro] = produto.estrut + '.' + produto.grupo + '.' + produto.subgrp + '.' + produto.itemat + '-' + produto.digmat,
        [Quantidade] = item.quatde,
        [Valor Unitário] = item.valite,
        [Total item] = item.totite
    from
        mat.MCT90000 cabecalho
    join mat.mct90100 item on
        item.numreg = cabecalho.numreg
        AND item.anoreg = cabecalho.anoreg
    JOIN mat.unidorcamentariaW uo on
        uo.idnivel5 = cabecalho.idnivel5
    JOIN mat.MXT62300 produto ON
        produto.estrut = item.estrut
        AND produto.grupo = item.grupo
        AND produto.subgrp = item.subgrp
        AND produto.itemat = item.itemat
        AND produto.digmat = item.digmat
    where
        cabecalho.anoreg >= %d
        and not exists(
        select
            1
        from
            mat.MCT79900 cotacao
        where
            cotacao.numreq = cabecalho.numreg
            and cotacao.anoreq = cabecalho.anoreg
            and cotacao.origem = 'R'
                                        )
    order by
        ano,
        numero,
        registropreco,
        item
    """ % (ANO - 1, ANO - 1))

    id_cadorc = 0
    chave_atual = ""
    numero = 0
    numorc = ""
    ano_atual = 0
    for row in tqdm(consulta, desc='Inserindo Solicitações...'):

        chave_cursor = str(row['numero']) + '/' + str(row['ano']) + '-' + row['registropreco']

        if chave_cursor != chave_atual:
            chave_atual = chave_cursor

            if ano_atual != row['ano']:
                ano_atual = row['ano']
                numero = 0

            id_cadorc += 1
            numero += 1
            num = f'{numero:05}'
            ano = row['ano']
            numorc = num + '/' + str(ano)[2:4]
            dtorc = row['dtorc']
            descr = row['descr'][:1024] if row['descr'] else row['descr']
            prioridade = row['prioridade']
            obs = row['obs']
            status = row['status']
            liberado = row['liberado']
            codccusto = row['codccusto']
            liberado_tela = row['liberado_tela']
            registropreco = row['registropreco']

            cur_fdb.execute(insert_cadorc, (
                id_cadorc, num, ano, numorc, dtorc, descr, prioridade, obs, status, liberado, codccusto, liberado_tela,
                EMPRESA, registropreco))

        item = row['item']
        codccusto = row['Requisitante']
        cadpro = PRODUTOS[row['cadpro']]
        qtd = float(row['Quantidade'])
        valor = float(row['Valor Unitário'])
        itemorc = row['item']
        itemorc_ag = row['item']

        cur_fdb.execute(insert_icadorc, (numorc, item, cadpro, qtd, valor, itemorc, codccusto, itemorc_ag, id_cadorc))
    commit()
