from modulos.compras import *

def cadunimedida():
    cur_fdb.execute("DELETE FROM CADEST")  # Limpa tabela
    cur_fdb.execute("DELETE FROM CADUNIMEDIDA")  # Limpa tabela
    cria_campo("ALTER TABLE CADUNIMEDIDA ADD codant_ant INTEGER")

    print("Inserindo Unidades de Medida...")

    cur_sql.execute("""
        SELECT upsigl,descricao,upcod FROM 
        (SELECT upsigl, rtrim(updesc) as descricao, upcod, ROW_NUMBER() OVER(PARTITION BY upsigl ORDER BY UPSIGL) SEQUENCIA FROM mat.MCT67900) UNID
        WHERE SEQUENCIA = 1
    """)  # Consulta banco de Origem

    insert = cur_fdb.prep("INSERT INTO CADUNIMEDIDA(sigla, descricao, codant_ant) VALUES(?,?,?)")  # Prepara o insert

    for row in tqdm(cur_sql.fetchall()):  # Para cada linha da consulta
        try:
            cur_fdb.execute(insert, (row[0], row[1], row[2]))  # Executa o insert
        except Exception as e:
            print(e)
            continue
    commit()  # Salva dados inseridos


def grupo_e_subgrupo():
    cria_campo("ALTER TABLE CADGRUPO ADD estrutura_ant varchar(2)")
    cria_campo("ALTER TABLE CADGRUPO ADD grupo_ant varchar(2)")
    cria_campo("ALTER TABLE CADSUBGR ADD estrutura_ant varchar(2)")
    cria_campo("ALTER TABLE CADSUBGR ADD grupo_ant varchar(2)")
    cria_campo("ALTER TABLE CADSUBGR ADD subgrupo_ant varchar(2)")
    cur_fdb.execute("DELETE FROM CADSUBGR")
    cur_fdb.execute("DELETE FROM CADGRUPO")

    consulta = fetchallmap("""select
                            estrut + right('0' + grupo,
                            2) as grupo,
                            right('00' + cast(subgrp as varchar(3)),
                            3) as subgrupo,
                            rtrim(desgrp) as nome,
                            estrut as estrutura_ant,
                            grupo as grupo_ant,
                            subgrp as subgrupo_ant
                        from
                            mat.mxt63300""")

    insert_grupo = cur_fdb.prep(
        "INSERT INTO CADGRUPO(grupo, nome, ocultar, grupo_ant, estrutura_ant) VALUES(?,?,?,?,?)")
    insert_subgrupo = cur_fdb.prep(
        "INSERT INTO CADSUBGR(grupo, subgrupo, nome, ocultar, grupo_ant, subgrupo_ant, estrutura_ant) VALUES(?,?,?,?,?,?,?)")

    for row in tqdm(consulta, desc='Inserindo Grupos e Subgrupos...'):
        if row['subgrupo'] == '000':
            cur_fdb.execute(insert_grupo, (row['grupo'], row['nome'], 'N', row['grupo_ant'], row['estrutura_ant']))
        cur_fdb.execute(insert_subgrupo, ( row['grupo'], row['subgrupo'], row['nome'][:45], 'N', row['grupo_ant'], row['subgrupo_ant'], row['estrutura_ant']))
    cur_fdb.execute(insert_grupo, ('112', 'Null', 'N', '12', '1'))
    commit()


def cadest():
    cria_campo("ALTER TABLE Cadest ADD estrut_ant int")
    cria_campo("ALTER TABLE Cadest ADD grupo_ant varchar(2)")
    cria_campo("ALTER TABLE Cadest ADD subgrupo_ant varchar(2)")
    cria_campo("ALTER TABLE Cadest ADD cod_ant varchar(14)")
    cria_campo("ALTER TABLE Cadest ADD tipopro_ant varchar(21)")
    cria_campo("DELETE FROM Cadest")

    i = 0

    consulta = fetchallmap("""select
            DISTINCT (codigo) cod_ant,
            grupo = SUBSTRING(Grupo, 1, 1) + SUBSTRING(Grupo, 3, 2),
            subgrp = SUBSTRING(codigo, 9, 1)+ SUBSTRING(Grupo, 6, 2),
            codigo = SUBSTRING(codigo, 10, 3),
            rtrim(descrição) disc1,
            CASE
                when [Tipo de Material] in ('Consumo', 'Acervo', 'Distribuição Gratuita', 'Móvel', 'Veículo') then 'P'
                else 'S'
            END tipopro,
            [Sigla Unidade Compra] unid1,
            [Especificação] discr1,
            'N' ocultar,
            SUBSTRING(Grupo, 1, 1) estrut_ant,
            SUBSTRING(Grupo, 3, 2) grupo_ant,
            SUBSTRING(Grupo, 6, 2) subgrupo_ant,
            [Tipo de Material] tipopro_ant,
            CASE
                when cast(SUBSTRING(codigo, 9, 4) as integer) % 1000 = 0 then 'S'
                else 'N'
            END extourou
        from
            (
            select
                [Código] = mx623.estrut + '.' + mx623.grupo + '.' + mx623.subgrp + '.' + mx623.itemat + '-' + mx623.digmat,
                [Grupo] = x633.estrut + '.' + x633.grupo + '.' + x633.subgrp + ' - ' + x633.desgrp,
                [Tipo de Material] = CASE
                    WHEN mx623.ictipmat = 'A' THEN 'Acervo'
                    WHEN mx623.ictipmat = 'C' THEN 'Consumo'
                    WHEN mx623.ictipmat = 'G' THEN 'Intangível'
                    WHEN mx623.ictipmat = 'M' THEN 'Móvel'
                    WHEN mx623.ictipmat = 'O' THEN 'Obras e Instalações'
                    WHEN mx623.ictipmat = 'D' THEN 'Distribuição Gratuita'
                    WHEN mx623.ictipmat = 'S' THEN 'Serviço'
                    WHEN mx623.ictipmat = 'V' THEN 'Veículo'
                    WHEN mx623.ictipmat = 'U' THEN 'Outros'
                END,
                [Descrição] = RTRIM(mx623.despro) + ISNULL(RTRIM(mx623.despro2),
                ''),
                [Sigla Unidade Compra] = mc679.upsigl,
                [Descrição Unidade Compra] = mc679.updesc,
                [Classe Patrimonial] = RTRIM(p068.CdClasse) + ' - ' + p068.Classe,
                [Especificação] = CONVERT(VARCHAR(8000),
                mx623.compl_descr),
                [Natureza de Despesa] = SUBSTRING(ele.cdelemdespesa, 1, 1) + '.' + SUBSTRING(ele.cdelemdespesa, 2, 1) + '.' + 
                                            SUBSTRING(ele.cdelemdespesa, 3, 2) + '.' + SUBSTRING(ele.cdelemdespesa, 5, 2) + '.' + 
                                            REPLICATE('0',
                2 - LEN(sub.cdsubelemdespesa)) + RTRIM(sub.cdsubelemdespesa)
            from
                mat.MXT62300 mx623
            INNER JOIN mat.MCT67900 mc679 ON
                mc679.upcod = mx623.upcod1
            INNER JOIN mat.MXT63300 x633 ON
                x633.estrut = mx623.estrut
                AND x633.grupo = mx623.grupo
                AND x633.subgrp = mx623.subgrp
            LEFT JOIN mat.MPV06800 p068 ON
                p068.idclspatrimonial = mx623.idclspatrimonial_incorporacao
            LEFT JOIN mat.MXT80300 x803 ON
                x803.idmaterial = mx623.idmaterial                
            LEFT JOIN mat.ElementoDespesa ele ON
                ele.idelemdespesa = x803.idelemdespesa
            LEFT JOIN mat.SubElementoDespesa sub ON
                sub.idelemdespesa = ele.idelemdespesa
                AND sub.idsubelemdespesa = x803.idsubelemdespesa) lma
        order by
            grupo,
            subgrp,
            codigo""")

    insert = cur_fdb.prep("""INSERT
								INTO
								Cadest(cadpro,
								grupo,
								subgrupo,
								codigo,
								disc1,
								tipopro,
								unid1,
								discr1,
								codreduz,
								ocultar,
								estrut_ant,
								grupo_ant,
								subgrupo_ant,
								cod_ant,
                                tipopro_ant)
							VALUES(?,?,?,?,?,
								?,?,?,?,?,
								?,?,?,?,?)"""
                          )

    for row in tqdm(consulta, desc='Inserindo Cadest...'):
        i += 1
        grupo = row['grupo']
        subgrp = row['subgrp']
        codigo = row['codigo']
        disc1 = row['disc1']
        tipopro = row['tipopro']
        unid1 = row['unid1']
        discr1 = row['discr1']
        codreduz = row['codigo']
        ocultar = row['ocultar']
        estrut_ant = row['estrut_ant']
        grupo_ant = row['grupo_ant']
        subgrupo_ant = row['subgrupo_ant']
        cod_ant = row['cod_ant']
        tipopro_ant = row['tipopro_ant']

        if row['extourou'] == 'S':
            nome_grupo = extourou_codigo_item(grupo, subgrupo_ant)
            sql = "INSERT INTO cadsubgr (grupo, subgrupo, nome, ocultar, grupo_ant, subgrupo_ant, estrutura_ant) VALUES (?, ?, ?, 'N', ?, ?, ?)"
            try:
                cur_fdb.execute(sql, (grupo, subgrp, nome_grupo, grupo_ant, subgrupo_ant, estrut_ant))
                commit()
            except Exception as e:
                print("Erro ao desdobrar subgrupo", e, nome_grupo)

        cadpro = f'{grupo}.{subgrp}.{codigo}'

        cur_fdb.execute(insert, ( cadpro, grupo, subgrp, codigo, disc1[:1024], tipopro, unid1, discr1, codreduz, ocultar, estrut_ant, grupo_ant, subgrupo_ant, cod_ant, tipopro_ant))
        commit() if i % 1000 == 0 else None
    commit()


def almoxarifado():
    cria_campo("ALTER TABLE DESTINO ADD cod_ant varchar(8)")
    cur_fdb.execute("DELETE FROM CENTROCUSTO")
    cur_fdb.execute("DELETE FROM DESTINO")

    consulta = fetchallmap(
        f"select RIGHT('000000000' + replace(almoxarifado, '.', ''),9) as destino, descricao, almoxarifado from mat.AlmoxValid av ")

    insert = cur_fdb.prep("insert into destino (COD, DESTI, EMPRESA, COD_ANT) values (?,?,?,?)")

    for row in tqdm(consulta, desc='Inserindo Almoxarifado...'):
        cur_fdb.execute(insert, (row['destino'], row['descricao'], EMPRESA, row['almoxarifado']))
    commit()

def centro_custo():
    cur_fdb.execute("DELETE FROM CENTROCUSTO")
    cria_campo('ALTER TABLE CENTROCUSTO ADD cod_ant varchar(19)')
    print("Inserindo Centros de Custo...")

    query = fetchallmap(f"""select
                                substring(nivel1,2,4) poder,
                                substring(nivel2,2,4)  orgao,
                                substring(nivel3,2,4) unidade,
                                rtrim(DescrUltimoNivel) descr,
                                rtrim(SiglaUltimoNivel) placa,
                                CASE 
                                    when ativa = 0 then 'S'
                                    else 'N'
                                END ocultar,
                                idNivel5 codccusto,
                                UnidOrc cod_ant
                            from
                                mat.UnidOrcamentariaW
                            where
                                ano = {ANO}
                            union all                    
                            select top 1
                                substring(nivel1,2,4) poder,
                                substring(nivel2,2,4)  orgao,
                                substring(nivel3,2,4) unidade,
                                'CONVERSÃO' descr,
                                '' placa,
                                'N' ocultar,
                                0 codccusto,
                                '0' cod_ant
                            from
                                mat.UnidOrcamentariaW
                            where
                                ano = {ANO}
    """)

    insert = cur_fdb.prep("""insert
                                into
                                centrocusto (poder,
                                orgao,
                                destino,
                                ccusto,
                                descr,
                                obs,
                                placa,
                                codccusto,
                                empresa,
                                unidade,
                                ocultar,
                                cod_ant)
                            values (?,?,?,?,?,?,?,?,?,?,?,?)""")

    for row in tqdm(query):
        poder = row['poder']
        orgao = row['orgao']
        destino = '000901001'
        ccusto = '001'
        descr = row['descr'][:60]
        obs = row['descr']
        placa = row['placa'][:7]
        codccusto = row['codccusto']
        empresa = EMPRESA
        unidade = row['unidade']
        ocultar = row['ocultar']
        codant = row['cod_ant']

        cur_fdb.execute(insert, (
            poder, orgao, destino, ccusto, descr, obs, placa, codccusto, empresa, unidade, ocultar, codant))
    commit()