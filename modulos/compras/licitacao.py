from modulos.compras import *

def cadlic(): # Cabeçalho de licitações
    cur_fdb.execute('delete from cadlic')
    cria_campo('ALTER TABLE CADLIC ADD criterio_ant varchar(30)')
    cria_campo('ALTER TABLE CADLIC ADD sigla_ant varchar(2)')
    cria_campo('ALTER TABLE CADLIC ADD status_ant varchar(1)')

    i = 0

    insert = cur_fdb.prep("""insert into cadlic (numpro,
                                datae,
                                dtpub,
                                dtenc,
                                horabe,
                                discr,
                                discr7,
                                modlic,
                                dthom,
                                dtadj,
                                comp,
                                numero,
                                ano,
                                registropreco,
                                ctlance,
                                obra,
                                proclic,
                                numlic,
                                liberacompra,
                                microempresa,
                                licnova,
                                tlance,
                                mult_entidade,
                                processo_ano,
                                LEI_INVERTFASESTCE,
                                criterio_ant,
                                sigla_ant,
                                status_ant,
                                codmod,
                                empresa,
                                valor,
                                detalhe,
                                anomod,
                                processo)
                            VALUES(?,?,?,?,?,
                                ?,?,?,?,?,
                                ?,?,?,?,?,
                                ?,?,?,?,?,
                                ?,?,?,?,?,
                                ?,?,?,?,?,?,?,?,?)""")
    
    consulta = fetchallmap(f"""SELECT
                                    RIGHT('000000' + CAST(ROW_NUMBER() OVER (
                                ORDER BY
                                    ano) AS VARCHAR),
                                    6)+ '/' + SUBSTRING(ano, 3, 2) proclic,
                                    RIGHT('000000' + CAST(ROW_NUMBER() OVER (
                                ORDER BY
                                    ano) AS VARCHAR),
                                    6) numero,
                                    *,
                                    CASE
                                        when modlic = 'DI01' then 1
                                        when modlic = 'CS01' then 7
                                        WHEN modlic = 'IN01' then 5
                                        when modlic = 'PE01' then 9
                                        when modlic = 'TOM6' then 3
                                        when modlic = 'CON7' THEN 4
                                        WHEN MODLIC = 'CON4' THEN 4
                                        WHEN MODLIC = 'PP01' THEN 8
                                        WHEN MODLIC = 'TOM3' THEN 3
                                    END codmod
                                FROM
                                    (
                                    SELECT
                                        c676.convit numpro,
                                        dtAtaAberturaProposta datae,
                                        dtpublicresult dtpub,
                                        dtence dtenc,
                                        '00:00' horabe,
                                        substring(ISNULL(ISNULL(notaconv, notaconvtxt), objeto_licitacao), 0, 1024) discr,
                                        ISNULL(ISNULL(notaconv,
                                        notaconvtxt),
                                        objeto_licitacao) detalhe,
                                        CASE
                                            WHEN CriterioJulgamento IN ('Dispensa', 'Inexigibilidade', 'Menor Preço') THEN 'Menor Preço Unitário'
                                            WHEN CriterioJulgamento IN ('Maior Lance ou Oferta', 'Maior Desconto') THEN 'Maior Desconto'
                                            WHEN CriterioJulgamento IN ('Taxa Administrativa') THEN 'Menor Acrescimo'
                                        END discr7,
                                        CASE
                                            WHEN c676.sigla IN (00) THEN 'DI01'
                                            --MODALIDADES DE LICITAÇÃO
                                            WHEN c676.sigla IN (01) THEN 'CS01'
                                            --CONCURSO
                                            WHEN c676.sigla IN (02) THEN 'CCO2'
                                            --siglae
                                            WHEN c676.sigla IN (03) THEN 'TOM3'
                                            --TOMADA 
                                            WHEN c676.sigla IN (04) THEN 'CON4'
                                            --CONCORRENCIA
                                            WHEN c676.sigla IN (05) THEN 'DI01'
                                            --DISPENSA
                                            WHEN c676.sigla IN (06) THEN 'IN01'
                                            --INEXIGIBILIDADE
                                            WHEN c676.sigla IN (07) THEN 'PP01'
                                            --Pregão Presencial
                                            WHEN c676.sigla IN (08) THEN 'PE01'
                                            --BEC - BOLSA ELETRONICA                            
                                            WHEN c676.sigla IN (09) THEN 'DI01'
                                            --OUTROS
                                            WHEN c676.sigla IN (10) THEN 'CCO5'
                                            --siglaE (Obras)
                                            WHEN c676.sigla IN (11) THEN 'DI01'
                                            --DISPENSA DE LICITAÇÃO - INCISO I                  
                                            WHEN c676.sigla IN (12) THEN 'DI01'
                                            --DISPENSA DE LICITAÇÃO - DEMAIS INCISOS            
                                            WHEN c676.sigla IN (13) THEN 'TOM6'
                                            --TOMADA DE PREÇOS (OBRAS E SERVIÇOS DE ENGENHARIA) 
                                            WHEN c676.sigla IN (14) THEN 'CON7'
                                            --CONCORRÊNCIA (OBRAS E SERVIÇOS DE ENGENHARIA)      
                                            WHEN c676.sigla IN (15) THEN 'DI01'
                                            --DISP
                                            WHEN c676.sigla IN (16) THEN 'DI01'
                                            --CHAMADA PUB
                                            WHEN c676.sigla IN (17) THEN 'DI01'
                                            --DISPENSA DE LICITAÇÃO - LEI Nº 14.133/2021     
                                            WHEN c676.sigla IN (18) THEN 'IN01'
                                            --INEXIGIBILIDADE - LEI Nº 14.133/2021    
                                            WHEN c676.sigla IN (19) THEN 'PE01'
                                            --Pregão - Eletrônico                                         
                                            WHEN c676.sigla IN (20) THEN 'CON4'
                                            --CONCORRÊNCIA ELETRÔNICA
                                            WHEN c676.sigla IN (21) THEN 'LEIL'
                                            --LEILÃO
                                            WHEN c676.sigla IN (22) THEN 'DI01'
                                            --Pregão Presencial
                                        END modlic,
                                        dtPublicacaoHomologacao dthom,
                                        dataadjudicacao dtadj,
                                        CASE
                                            WHEN status IN ('R') THEN 7
                                            WHEN status IN ('F', 'C') THEN 6
                                            WHEN status IN ('D') THEN 5
                                            WHEN status IN ('H', 'X', 'P') THEN 3
                                            WHEN status in ('U') THEN 8
                                            WHEN status IN ('A') THEN 2
                                            ELSE 1
                                        END comp,
                                        c676.anoc ano,
                                        'N' registropreco,
                                        'U' ctlance,
                                        CASE
                                            WHEN c676.sigla IN (10, 13, 14) THEN 'S'
                                            ELSE 'N'
                                        END obra,
                                        idagenda numlic,
                                        CASE
                                            WHEN status IN ('H', 'X', 'P') THEN 'S'
                                            ELSE NULL
                                        END liberacompra,
                                        2 microempresa,
                                        1 licnova,
                                        '$' tlance,
                                        'N' mult_entidade,
                                        c676.anoc processo_ano,
                                        'N' LEI_INVERTFASESTCE,
                                        valorEstimado valor,
                                        CriterioJulgamento criterio_ant,
                                        c676.sigla sigla_ant,
                                        status status_ant,
                                        SUBSTRING(RTRIM(cpcpro), PATINDEX('%[^0]%', RTRIM(cpcpro) + '.'), LEN(cpcpro)) AS processo
                                    FROM
                                        MAT.MCT67600 c676
                                    left join mat.MCT80600 c806 on
                                        c806.convit = c676.convit
                                        and c806.anoc = c676.anoc
                                        and c806.sigla = c676.sigla
                                    WHERE
                                        c676.anoc >= {ANO-5}
                                union ALL
                                    SELECT
                                        c914.convit numpro,
                                        c914.dtAtaAberturaProposta datae,
                                        c914.dtpublicresult dtpub,
                                        c914.dtence dtenc,
                                        '00:00' horabe,
                                        substring(ISNULL(ISNULL(objeto_licitacao, notaconv), nota2), 0, 1024) discr,
                                        ISNULL(ISNULL(objeto_licitacao,
                                        notaconv),
                                        nota2) detalhe,
                                        'Menor Preço Unitário' discr7,
                                        CASE
                                            WHEN c914.sigla IN (00) THEN 'DI01'
                                            --MODALIDADES DE LICITAÇÃO
                                            WHEN c914.sigla IN (01) THEN 'CS01'
                                            --CONCURSO
                                            WHEN c914.sigla IN (02) THEN 'CCO2'
                                            --siglae
                                            WHEN c914.sigla IN (03) THEN 'TOM3'
                                            --TOMADA 
                                            WHEN c914.sigla IN (04) THEN 'CON4'
                                            --CONCORRENCIA
                                            WHEN c914.sigla IN (05) THEN 'DI01'
                                            --DISPENSA
                                            WHEN c914.sigla IN (06) THEN 'IN01'
                                            --INEXIGIBILIDADE
                                            WHEN c914.sigla IN (07) THEN 'PP01'
                                            --Pregão Presencial
                                            WHEN c914.sigla IN (08) THEN 'PE01'
                                            --BEC - BOLSA ELETRONICA                            
                                            WHEN c914.sigla IN (09) THEN 'DI01'
                                            --OUTROS
                                            WHEN c914.sigla IN (10) THEN 'CCO5'
                                            --siglaE (Obras)
                                            WHEN c914.sigla IN (11) THEN 'DI01'
                                            --DISPENSA DE LICITAÇÃO - INCISO I                  
                                            WHEN c914.sigla IN (12) THEN 'DI01'
                                            --DISPENSA DE LICITAÇÃO - DEMAIS INCISOS            
                                            WHEN c914.sigla IN (13) THEN 'TOM6'
                                            --TOMADA DE PREÇOS (OBRAS E SERVIÇOS DE ENGENHARIA) 
                                            WHEN c914.sigla IN (14) THEN 'CON7'
                                            --CONCORRÊNCIA (OBRAS E SERVIÇOS DE ENGENHARIA)      
                                            WHEN c914.sigla IN (15) THEN 'DI01'
                                            --DISP
                                            WHEN c914.sigla IN (16) THEN 'DI01'
                                            --CHAMADA PUB
                                            WHEN c914.sigla IN (17) THEN 'DI01'
                                            --DISPENSA DE LICITAÇÃO - LEI Nº 14.133/2021     
                                            WHEN c914.sigla IN (18) THEN 'IN01'
                                            --INEXIGIBILIDADE - LEI Nº 14.133/2021    
                                            WHEN c914.sigla IN (19) THEN 'PE01'
                                            --Pregão - Eletrônico                                         
                                            WHEN c914.sigla IN (20) THEN 'CON4'
                                            --CONCORRÊNCIA ELETRÔNICA
                                            WHEN c914.sigla IN (21) THEN 'LEIL'
                                            --LEILÃO
                                            WHEN c914.sigla IN (22) THEN 'DI01'
                                            --Pregão Presencial
                                        END modlic,
                                        dtPublicacaoHomologacao dthom,
                                        dataadjudicacao dtadj,
                                        CASE
                                            WHEN status IN ('R') THEN 7
                                            WHEN status IN ('F', 'C') THEN 6
                                            WHEN status IN ('D') THEN 5
                                            WHEN status IN ('H', 'X', 'P') THEN 3
                                            WHEN status in ('U') THEN 8
                                            WHEN status IN ('A') THEN 2
                                            ELSE 1
                                        END comp,
                                        c914.anoc ano,
                                        'S' registropreco,
                                        'U' ctlance,
                                        CASE
                                            WHEN c914.sigla IN (10, 13, 14) THEN 'S'
                                            ELSE 'N'
                                        END obra,
                                        idagendaRP numlic,
                                        CASE
                                            WHEN status IN ('H', 'X', 'P') THEN 'S'
                                            ELSE NULL
                                        END liberacompra,
                                        2 microempresa,
                                        1 licnova,
                                        '$' tlance,
                                        'N' mult_entidade,
                                        c914.anoc processo_ano,
                                        'N' LEI_INVERTFASESTCE,
                                        valorEstimado valor,
                                        CriterioAceitabilidade criterio_ant,
                                        c914.sigla sigla_ant,
                                        status status_ant,
                                        SUBSTRING(RTRIM(cpcpro), PATINDEX('%[^0]%', RTRIM(cpcpro) + '.'), LEN(cpcpro)) AS processo
                                    FROM
                                        mat.MCT91400 c914
                                    left join mat.MCT90300 c903 on
                                        c903.convit = c914.convit
                                        and c903.anoc = c914.anoc
                                        and c903.sigla = c914.sigla
                                    where
                                        c914.anoc >= {ANO-5}
                                                                ) AS subconsulta
                                ORDER BY
                                    proclic,
                                    ANO;""")
    
    for row in tqdm(consulta, desc='Inserindo Cadastro de licitações...'):
        i += 1
        numpro = int(row['numpro'])
        datae = row['datae']
        dtpub = row['dtpub']
        dtenc = row['dtenc']
        horabe = row['horabe']
        discr = row['discr']
        discr7 = row['discr7']
        modlic = row['modlic']
        dthom = row['dthom']
        dtadj = row['dtadj']
        comp = row['comp']
        numero = row['numero']
        ano = row['ano']
        registropreco = row['registropreco']
        ctlance = row['ctlance']
        obra = row['obra']
        proclic = row['proclic']
        numlic = row['numlic']
        liberacompras = row['liberacompra']
        microempresa = row['microempresa']
        licnova = row['licnova']
        tlance = row['tlance']
        mult_entidade = row['mult_entidade']
        processo_ano = row['processo_ano']
        LEI_INVERTFASESTCE = row['LEI_INVERTFASESTCE']
        criterio_ant = row['criterio_ant']
        sigla_ant = row['sigla_ant']
        status_ant = row['status_ant']
        codmod = row['codmod']
        empresa = EMPRESA
        valor = row['valor']
        detalhe = row['detalhe']
        anomod = row['ano']
        processo = row['processo']

        cur_fdb.execute(insert,(numpro, datae, dtpub, dtenc, horabe, discr, discr7, modlic, dthom, dtadj, comp, numero, ano, registropreco, ctlance, obra, proclic, numlic, liberacompras, microempresa,
                                 licnova, tlance, mult_entidade, processo_ano, LEI_INVERTFASESTCE, criterio_ant, sigla_ant, status_ant, codmod, empresa, valor, detalhe, anomod, processo))
        
        if i % 1000 == 0:
            commit()
        
    cur_fdb.execute('''INSERT INTO CADLIC_SESSAO (NUMLIC, SESSAO, DTREAL, HORREAL, COMP, DTENC, HORENC, SESSAOPARA, MOTIVO) 
                  SELECT L.NUMLIC, CAST(1 AS INTEGER), L.DTREAL, L.HORREAL, L.COMP, L.DTENC, L.HORENC, CAST('T' AS VARCHAR(1)), CAST('O' AS VARCHAR(1)) FROM CADLIC L 
                  WHERE numlic not in (SELECT FIRST 1 S.NUMLIC FROM CADLIC_SESSAO S WHERE S.NUMLIC = L.NUMLIC)''')
    cur_fdb.execute('''UPDATE cadlic SET MASCMOD = SIGLA_ANT||'-'||NUMPRO||'/'||ANO''')
    cur_fdb.execute('UPDATE CONTRATOS a SET a.PROCLIC = (SELECT b.proclic FROM cadlic b WHERE b.processo = a.proclic AND a.ano = b.ano and a.codif = b.codif)')
    commit()

LICITACAO = licitacoes()
def cadprolic(): # Itens de licitações
    cur_fdb.execute('DELETE FROM CADPROLIC')
    cria_campo('ALTER TABLE ICADORC ADD numlic varchar(10)')
    vincula_cotacao_licitacao()
    cur_fdb.execute('UPDATE ICADORC a SET a.numlic = (SELECT b.numlic FROM cadorc b WHERE a.NUMORC=b.numorc AND b.numlic IS NOT null)')
    commit()

    cur_fdb.execute("""INSERT INTO cadprolic (item, item_mask, numorc, cadpro, quan1, vamed1, vatomed1, codccusto, reduz, numlic, microempresa, tlance, item_ag, id_cadorc)
	                   SELECT item, item, numorc, cadpro, qtd, valor, qtd * valor total, CODCCUSTO, 'N' reduz,	numlic, 'N' microempresa, '$' tlance, ITEMORC_AG, ID_CADORC
                       FROM iCADORC c WHERE numlic IS NOT NULL""")

    cur_fdb.execute('''INSERT INTO CADPROLIC_DETALHE (NUMLIC,item,CADPRO,quan1,VAMED1,VATOMED1,marca,CODCCUSTO,ITEM_CADPROLIC) 
                       select numlic, item, cadpro, quan1, vamed1, vatomed1, marca, codccusto, item from cadprolic''')

NOME_FORNECEDOR, INSMF_FORNECEDOR = fornecedores()
def prolic_prolics(): # Proponentes de licitações
    cur_fdb.execute('DELETE FROM PROLICS')
    cur_fdb.execute('DELETE FROM PROLIC')
    cria_campo('alter table prolics add codif_ant varchar(50)')

    i = 0

    global INSMF_FORNECEDOR
    global NOME_FORNECEDOR

    consulta = fetchallmap(f"""select
                                    distinct query.*, rtrim(t614.dcto01) insmf
                                from
                                    (
                                    select
                                        cast(convit as integer) numpro,
                                        sigla sigla_ant,
                                        anoc ano,
                                        cast(codfor as varchar) codif,
                                        case
                                            when selecao = 0 then 'D'
                                            else 'A'
                                        end status,
                                        'N' usa_preferencia,
                                        null nome_ant
                                    from
                                        mat.MCT70100
                                union all
                                    select
                                        cast(convit as integer) numpro,
                                        sigla sigla_ant,
                                        anoc ano,
                                        cast(codfor as integer) codif,
                                        'A' status,
                                        'N' usa_preferencia,
                                        null nome_ant
                                    from
                                        mat.mct90400
                                union all
                                    select
                                        cast(convit as integer) numpro,
                                        sigla sigla_ant,
                                        anoc ano,
                                        NULL codif,
                                        'A' status,
                                        CASE
                                            when direitoPreferencia = 1 then 'S'
                                            else 'N'
                                        END usa_preferencia,
                                        substring(razao_social, 0, 40) nome_ant
                                    from
                                        mat.MCT81800 m
                                    where
                                        documento is not null
                                union ALL
                                    SELECT
                                        cast(convit as integer) numpro,
                                        sigla sigla_ant,
                                        anoc ano,
                                        cast(codfor as integer) codif,
                                        'A' status,
                                        CASE
                                            when direitoPreferencia = 1 then 'S'
                                            else 'N'
                                        END usa_preferencia,
                                        substring(razao_social, 0, 40) nome_ant
                                    from
                                        mat.MCT81800 m
                                    where
                                        codfor is not null
                                        and codfor <> '') as query
                                left join mat.MXT60100 t601 on
                                    t601.codfor = [codif]
                                left join mat.MXT61400 t614 on
                                    t614.codnom = t601.codnom
                                where
                                    ano >= {ANO-5}
                                    and status = 'A'""")

    insert_prolic = cur_fdb.prep('insert into prolic (codif, nome, status, numlic) values (?,?,?,?)')
    insert_prolics = cur_fdb.prep('insert into prolics (sessao, codif, status, representante, numlic, usa_preferencia, codif_ant) values (?,?,?,?,?,?,?)')

    for row in tqdm(consulta, desc='Inserindo Proponentes...'):
        i += 1
        try:
            codif = INSMF_FORNECEDOR.get(row['insmf'], row['codif']) 
            nome = NOME_FORNECEDOR[codif]
        except:
            filtro = cadastra_fornecedor_especifico(row['insmf'], row['codif'])
            codif = filtro[0]
            nome = filtro[1]
            INSMF_FORNECEDOR[row['insmf']] = codif
            NOME_FORNECEDOR[codif] = nome
        status = row['status']
        usa_preferencia = row['usa_preferencia']
        codif_ant = row['codif']

        try:
            numlic = LICITACAO[(row['numpro'], row['sigla_ant'], row['ano'])]
            cur_fdb.execute(insert_prolic,(codif, nome, status, numlic))
            cur_fdb.execute(insert_prolics,(1,codif, status, nome, numlic, usa_preferencia,nome,codif_ant))
            if i % 1000 == 0:
                commit()
        except Exception as e:
            continue
    commit()

def cadpro_status(): # Status de licitações
    cur_fdb.execute('DELETE FROM CADPRO_STATUS')

    consulta = cur_fdb.execute("""SELECT
                                    1 sessao,
                                    a.LOTELIC,
                                    item,
                                    CASE WHEN b.COMP = 3 THEN 'I_ENCERRAMENTO' ELSE NULL END AS TELAFINAL,
                                    CASE WHEN b.COMP = 3 THEN 'S' ELSE NULL END AS ACEITO,
                                    b.NUMLIC 
                                FROM
                                    CADPROLIC a
                                JOIN cadlic b ON
                                    a.NUMLIC = b.NUMLIC""").fetchallmap()
    
    insert = cur_fdb.prep('insert into cadpro_status (numlic, sessao, itemp, item) values (?,?,?,?)')

    for row in tqdm(consulta, desc='Inserindo Stauts...'):
        numlic = row['numlic']
        itemp = row['item']
        item = row['item']

        cur_fdb.execute(insert,(numlic, '1', itemp, item))
    cur_fdb.execute("update cadlic set liberacompra = 'S' where comp = 3 and status_ant in ('X','H','P')")
    cur_fdb.execute("update cadlic set liberacompra = 'N' where comp = 3 and status_ant in ('A')")
    commit()

def cadpro_proposta(): # Propostas de licitações
    cur_fdb.execute('DELETE FROM cadpro_proposta')
    commit()

    ###### SELECT DA PROPOSTA COM ITENS DESAGRUPADOS
    consulta = fetchallmap(f"""select distinct a.*, isnull(rtrim(t614.dcto01),a.[codif]) insmf from (select DISTINCT * from (select
                                    1 sessao,
                                    codfor codif,
                                    --ROW_NUMBER() over (partition by isnull(codfor,insmf), convit order by nuitem) item,
                                    nuitem itemp,
                                    coalesce(qtde, 0) quan1,
                                    coalesce(preco, 0) vaun1,
                                    coalesce(total, 0) vato1,
                                    case when venc is null then 'D' else 'C' end as status,
                                    venc subem,
                                    rtrim(marca) marca,
                                    right('00000000'+cast(nrolote as varchar),8) lotelic,
                                    sigla sigla_ant,
                                    convit numpro,
                                    anoc ano,
                                    'N' registropreco
                                from
                                    (
                                    SELECT 
                                        c697.IdProcCompra,
                                        c697.unges,
                                        c697.sigla,
                                        c697.convit,
                                        c697.anoc,
                                        c803.idlote,
                                        nrolote = CASE
                                            WHEN c803.idLote IS NULL THEN c698.nuitem
                                            ELSE c934.NroLote
                                        END,
                                        descricao = CASE
                                            WHEN c803.idLote IS NULL THEN 'Lote ' + RTRIM(c698.nuitem)
                                            ELSE c934.Descricao
                                        END,
                                        estrut = CASE
                                            WHEN c812.estrut_atu IS NULL THEN c698.estrut
                                            ELSE c812.estrut_atu
                                        END,
                                        grupo = CASE
                                            WHEN c812.grupo_atu IS NULL THEN c698.grupo
                                            ELSE c812.grupo_atu
                                        END,
                                        subgrp = CASE
                                            WHEN c812.subgrp_atu IS NULL THEN c698.subgrp
                                            ELSE c812.subgrp_atu
                                        END,
                                        itemat = CASE
                                            WHEN c812.itemat_atu IS NULL THEN c698.itemat
                                            ELSE c812.itemat_atu
                                        END,
                                        digmat = CASE
                                            WHEN c812.digmat_atu IS NULL THEN c698.digmat
                                            ELSE c812.digmat_atu
                                        END,
                                        codfor = c698.codfor,
                                        c698.codfor_representante,
                                        c698.venc,
                                        c698.empate,
                                        c698.preco,
                                        c698.marca,
                                        c698.valid,
                                        c698.prazo,
                                        c698.pgto,
                                        c698.nuitem,
                                        c698.garantia,
                                        qtde = SUM(C803.quantid),
                                        total = SUM(ROUND(C803.quantid * c698.preco, 2)),
                                        insmf = c072.nrcpfcnpj
                                    FROM
                                        mat.MCT69700 c697
                                    INNER JOIN mat.MCT69800 c698 ON
                                        c698.IdProcCompra = c697.IdProcCompra
                                    INNER JOIN mat.MCT80200 c802 ON
                                        C802.convit = c697.convit
                                        AND C802.sigla = c697.sigla
                                        AND C802.anoc = c697.anoc
                                        AND C802.unges = c697.unges
                                        AND c802.aditivo = 0
                                    INNER JOIN mat.MCT80300 c803 ON
                                        C803.codgrupo = C802.codgrupo
                                        AND C803.anogrupo = C802.anogrupo
                                        AND C803.unges = C802.unges
                                        AND C803.estrut = c698.estrut
                                        AND C803.grupo = c698.grupo
                                        AND C803.subgrp = c698.subgrp
                                        AND C803.itemat = c698.itemat
                                        AND C803.digmat = c698.digmat
                                        AND ISNULL(c803.idLote,
                                        0) = ISNULL(c698.idLote,
                                        0)
                                    LEFT JOIN mat.MCT81200 c812 ON
                                        c812.unges = c697.unges
                                        AND c812.sigla = c697.sigla
                                        AND c812.anoc = c697.anoc
                                        AND c812.convit = c697.convit
                                        AND c812.codfor = c698.codfor
                                        AND c812.estrut_ant = c698.estrut
                                        AND c812.grupo_ant = c698.grupo
                                        AND c812.subgrp_ant = c698.subgrp
                                        AND c812.itemat_ant = c698.itemat
                                        AND c812.digmat_ant = c698.digmat
                                    LEFT JOIN mat.MCT93400 c934 ON
                                        c934.IdLote = c803.idLote
                                    LEFT JOIN mat.MCT07200 c072 ON
                                        c072.idfornecedor = c698.idMCT072
                                    where c697.anoc >= {ANO-5}
                                    GROUP BY
                                        c697.IdProcCompra,
                                        c697.unges,
                                        c697.sigla,
                                        c697.convit,
                                        c697.anoc,
                                        c812.estrut_atu,
                                        c812.grupo_atu,
                                        c812.subgrp_atu,
                                        c812.itemat_atu,
                                        c812.digmat_atu,
                                        c698.estrut,
                                        c698.grupo,
                                        c698.subgrp,
                                        c698.itemat,
                                        c698.digmat,
                                        c698.codfor,
                                        c698.venc,
                                        c698.empate,
                                        c698.preco,
                                        c698.marca,
                                        c698.valid,
                                        c698.prazo,
                                        c698.pgto,
                                        c698.nuitem,
                                        c698.garantia,
                                        c803.idlote,
                                        c934.nrolote,
                                        c934.descricao,
                                        c698.codfor_representante,
                                        c072.nrcpfcnpj) as query
                                Union all
                                select
                                    1 sessao,
                                    codfor codif,
                                    --ROW_NUMBER() over (partition by codfor, convit order by nuitem)itemp,
                                    nuitem,
                                    coalesce(qtde,0) quan1,
                                    coalesce(preco,0) vaun1,
                                    coalesce(total,0) vato1,
                                    case when isnull(class,venc) is null then 'D' else 'C' end as status,
                                    venc subem,
                                    rtrim(marca) marca,
                                    right('00000000'+cast(nrolote as varchar),8) lotelic,
                                    sigla sigla_ant,
                                    convit numpro,
                                    anoc ano,
                                    'S' registropreco
                                from
                                    (
                                    SELECT
                                        c905.unges,
                                        c905.sigla,
                                        c905.convit,
                                        c905.anoc,
                                        c913.idLote,
                                        nrolote = CASE
                                            WHEN c913.idLote IS NULL THEN c905.nuitem
                                            ELSE c934.NroLote
                                        END,
                                        descricao = CASE
                                            WHEN c913.idLote IS NULL THEN 'Lote ' + RTRIM(c905.nuitem)
                                            ELSE c934.Descricao
                                        END,
                                        estrut = CASE
                                            WHEN c812.estrut_atu IS NULL THEN c905.estrut
                                            ELSE c812.estrut_atu
                                        END,
                                        grupo = CASE
                                            WHEN c812.grupo_atu IS NULL THEN c905.grupo
                                            ELSE c812.grupo_atu
                                        END,
                                        subgrp = CASE
                                            WHEN c812.subgrp_atu IS NULL THEN c905.subgrp
                                            ELSE c812.subgrp_atu
                                        END,
                                        itemat = CASE
                                            WHEN c812.itemat_atu IS NULL THEN c905.itemat
                                            ELSE c812.itemat_atu
                                        END,
                                        digmat = CASE
                                            WHEN c812.digmat_atu IS NULL THEN c905.digmat
                                            ELSE c812.digmat_atu
                                        END,
                                        c905.codfor,
                                        c905.venc,
                                        c905.class,
                                        preco = c905.pr_unit,
                                        c905.marca,
                                        c905.modelo,
                                        c905.nuitem,
                                        qtde = SUM(c913.quantid),
                                        total = SUM(ROUND(c905.qtde * c905.pr_unit, 2)),
                                        insmf = c072.nrcpfcnpj
                                    FROM
                                        mat.MCT90500 c905
                                    INNER JOIN mat.MCT91200 c912 ON
                                        c912.unges = c905.unges
                                        AND c912.sigla = c905.sigla
                                        AND c912.convit = c905.convit
                                        AND c912.anoc = c905.anoc
                                    INNER JOIN mat.MCT91300 c913 ON
                                        c913.unges = c912.unges
                                        AND c913.codgrupo = c912.codgrupo
                                        AND c913.anogrupo = c912.anogrupo
                                        AND c913.estrut = c905.estrut
                                        AND c913.grupo = c905.grupo
                                        AND c913.subgrp = c905.subgrp
                                        AND c913.itemat = c905.itemat
                                        AND c913.digmat = c905.digmat
                                        AND ISNULL(c913.idLote,
                                        0) = ISNULL(c905.idLote,
                                        0)
                                    LEFT JOIN mat.MCT81200 c812 ON
                                        c812.unges = c905.unges
                                        AND c812.sigla = c905.sigla
                                        AND c812.anoc = c905.anoc
                                        AND c812.convit = c905.convit
                                        AND c812.codfor = c905.codfor
                                        AND c812.estrut_ant = c905.estrut
                                        AND c812.grupo_ant = c905.grupo
                                        AND c812.subgrp_ant = c905.subgrp
                                        AND c812.itemat_ant = c905.itemat
                                        AND c812.digmat_ant = c905.digmat
                                    LEFT JOIN mat.MCT93400 c934 ON
                                        c934.IdLote = c913.idLote
                                    LEFT JOIN mat.MCT07200 c072 ON
                                        c072.idfornecedor = c905.idMCT072
                                    where c905.anoc >= {ANO-5}
                                    GROUP BY
                                        c905.unges,
                                        c905.sigla,
                                        c905.convit,
                                        c905.anoc,
                                        c913.idLote,
                                        c812.estrut_atu,
                                        c812.grupo_atu,
                                        c812.subgrp_atu,
                                        c812.itemat_atu,
                                        c812.digmat_atu,
                                        c905.estrut,
                                        c905.grupo,
                                        c905.subgrp,
                                        c905.itemat,
                                        c905.digmat,
                                        c905.pr_unit,
                                        c913.idLote,
                                        c905.codfor,
                                        c905.venc,
                                        c905.class,
                                        c905.nuitem,
                                        c905.marca,
                                        c905.modelo,
                                        c934.nrolote,
                                        c934.descricao,
                                        c072.nrcpfcnpj) as query) as rn) as a
                                        left join mat.MXT60100 t601 on
                                    	t601.codfor = [codif]
		                                left join mat.MXT61400 t614 on
		                                t614.codnom = t601.codnom
                                        order by [subem] desc""")

    insert = cur_fdb.prep('insert into cadpro_proposta (codif, sessao, numlic, itemp, item, quan1, vaun1, vato1, status, marca, subem) values (?,?,?,?,?,?,?,?,?,?,?)')
    i = 0

    for row in tqdm(consulta, desc='Inserindo Propostas...'):
        i += 1
        try:
            codif = INSMF_FORNECEDOR.get(row['insmf'], row['codif']) 
        except:
            codif = cadastra_fornecedor_especifico(row['insmf'], row['codif'])
            INSMF_FORNECEDOR[row['insmf']] = codif # Atualiza o dicionário
        sessao = row['sessao']
        numlic = LICITACAO[(row['numpro'], row['sigla_ant'], row['ano'])] #, row['registropreco']
        itemp = row['itemp']
        item = row['itemp']
        quan1 = int(row['quan1'])
        vaun1 = float(row['vaun1'])
        vato1 = float(row['vato1'])
        status = row['status']
        marca = row['marca']
        subem = row['subem']

        try:
            cur_fdb.execute(insert,(codif, sessao, numlic, itemp, item, quan1, vaun1, vato1, status, marca, subem))
        except:
            continue

        if i % 1000 == 0:
            commit()
    commit()

def cadpro_lance(): # Lances de licitações
    cur_fdb.execute('delete from cadpro_lance')
    cria_campo("alter table cadpro_lance add marca varchar(50)")
    commit()
    print('Inserindo os lances...')
    cur_fdb.execute("""insert into cadpro_lance (sessao, rodada, codif, itemp, vaunl, vatol, status, subem, numlic, marca)
                        SELECT sessao, 1 rodada, CODIF, ITEMP, VAUN1, VATO1, 'F' status, SUBEM, numlic, marca FROM CADPRO_PROPOSTA cp where subem = 1""")
    commit()

def cadpro_final(): # Situação final de licitações
    print('Inserindo os dados finais...')
    cria_campo("alter table cadpro_final add CQTDADT double precision")
    cria_campo("alter table cadpro_final add ccadpro varchar(20)")
    cria_campo("alter table cadpro_final add CCODCCUSTO integer;")
    cur_fdb.execute('delete from cadpro_final')
    commit()

    cur_fdb.execute("""EXECUTE BLOCK
                        AS
                        BEGIN	
                            INSERT INTO CADPRO_FINAL (NUMLIC, ULT_SESSAO, CODIF, ITEMP, VAUNF, VATOF, STATUS, SUBEM, PERCF)
                                                SELECT A.NUMLIC, A.SESSAO, A.CODIF, A.ITEMP, A.VAUNL, A.VATOL, 'C', 1, NULL 
                                                FROM CADPRO_LANCE A  
                                                WHERE NOT EXISTS(SELECT 1 FROM CADPRO_FINAL B WHERE A.NUMLIC = B.NUMLIC AND A.SESSAO = B.ULT_SESSAO AND A.CODIF = B.CODIF AND A.ITEMP = B.ITEMP)  
                                                AND A.STATUS = 'F' AND A.NUMLIC IN (SELECT NUMLIC FROM CADLIC);                            
                            INSERT INTO CADPRO_FINAL (NUMLIC, ULT_SESSAO, CODIF, ITEMP, VAUNF, VATOF, STATUS, SUBEM, PERCF) 
                                                SELECT A.NUMLIC, A.SESSAO, A.CODIF, A.ITEMP, A.VAUN1, A.VATO1, 'C', 1, NULL  
                                                FROM CADPRO_PROPOSTA A 
                                                WHERE NOT EXISTS(SELECT 1 FROM CADPRO_FINAL B WHERE A.NUMLIC = B.NUMLIC AND A.SESSAO = B.ULT_SESSAO AND A.ITEMP = B.ITEMP) 
                                                AND A.STATUS = 'C' AND A.SUBEM = 1 AND A.NUMLIC IN (SELECT NUMLIC FROM CADLIC);
                            UPDATE CADPRO_FINAL A SET A.CQTDADT = (SELECT B.QUAN1 FROM CADPROLIC B WHERE A.NUMLIC = B.NUMLIC AND A.ITEMP = B.ITEM) 
                                            WHERE A.NUMLIC IN (SELECT C.NUMLIC FROM CADLIC C);                              
                            UPDATE CADPRO_FINAL A SET A.CCADPRO = (SELECT B.CADPRO FROM CADPROLIC B WHERE A.NUMLIC = B.NUMLIC AND A.ITEMP = B.ITEM) 
                                            WHERE A.NUMLIC IN (SELECT C.NUMLIC FROM CADLIC C);                              
                            UPDATE CADPRO_FINAL A SET A.CCODCCUSTO = (SELECT B.CODCCUSTO FROM CADPROLIC B WHERE A.NUMLIC = B.NUMLIC AND A.ITEMP = B.ITEM) 
                                            WHERE A.NUMLIC IN (SELECT C.NUMLIC FROM CADLIC C);        
                        END""")
    commit()

def cadpro(): # Saldo Homologado da licitação
    print('Inserindo Cadpro...')
    cur_fdb.execute('delete from cadpro')
    
    cur_fdb.execute(f"""INSERT
                            INTO
                            CADPRO(CODIF,
                            CADPRO,
                            QUAN1,
                            VAUN1,
                            VATO1,
                            SUBEM,
                            STATUS,
                            ITEM,
                            NUMORC,
                            ITEMORCPED,
                            CODCCUSTO,
                            FICHA,
                            ELEMENTO,
                            DESDOBRO,
                            NUMLIC,
                            ULT_SESSAO,
                            ITEMP,
                            QTDADT,
                            QTDPED,
                            VAUNADT,
                            VATOADT,
                            PERC,
                            QTDSOL,
                            ID_CADORC,
                            VATOPED,
                            VATOSOL,
                            TPCONTROLE_SALDO,
                            QTDPED_FORNECEDOR_ANT,
                            VATOPED_FORNECEDOR_ANT,
                            marca)
                                                SELECT
                            a.CODIF,
                            c.CADPRO,
                            CASE WHEN VAUNL <> 0 THEN round((a.vatol / a.VAUNL), 2) ELSE 0 END qtdunit,
                            a.VAUNL,
                            CASE WHEN VAUNL <> 0 THEN round((a.vatol / a.VAUNL), 2) * a.VAUNL ELSE 0 END VATOTAL,
                            1,
                            'C',
                            c.ITEM,
                            c.NUMORC,
                            c.ITEM,
                            c.CODCCUSTO,
                            c.FICHA,
                            c.ELEMENTO,
                            c.DESDOBRO,
                            a.NUMLIC,
                            1,
                            b.ITEMP,
                            CASE WHEN VAUNL <> 0 THEN round((a.vatol / a.VAUNL), 2) ELSE 0 END qtdunit_adit,
                            0,
                            a.VAUNL,
                            CASE WHEN VAUNL <> 0 THEN round((a.vatol / a.VAUNL), 2) * a.VAUNL ELSE 0 END VATOTAL,
                            0,
                            0,
                            c.ID_CADORC,
                            0,
                            0,
                            'Q',
                            0,
                            0,
                            a.marca
                        FROM
                            CADPRO_LANCE a
                        INNER JOIN CADPRO_STATUS b ON
                            b.NUMLIC = a.NUMLIC
                            AND a.ITEMP = b.ITEMP
                            AND a.SESSAO = b.SESSAO
                        INNER JOIN CADPROLIC_DETALHE c ON
                            c.NUMLIC = a.NUMLIC
                            AND b.ITEM = c.ITEM_CADPROLIC
                        INNER JOIN CADLIC D ON
                            D.NUMLIC = A.NUMLIC
                        WHERE
                            a.SUBEM = 1
                            AND a.STATUS = 'F'""")
    commit()

def regpreco(): # Saldo Homologado da licitação de RP
    cur_fdb.execute("""EXECUTE BLOCK AS  
                        BEGIN  
                        INSERT INTO REGPRECODOC (NUMLIC, CODATUALIZACAO, DTPRAZO, ULTIMA)  
                        SELECT DISTINCT A.NUMLIC, 0, DATEADD(1 YEAR TO A.DTHOM), 'S'  
                        FROM CADLIC A WHERE A.REGISTROPRECO = 'S' AND A.DTHOM IS NOT NULL  
                        AND NOT EXISTS(SELECT 1 FROM REGPRECODOC X  
                        WHERE X.NUMLIC = A.NUMLIC);  

                        INSERT INTO REGPRECO (COD, DTPRAZO, NUMLIC, CODIF, CADPRO, CODCCUSTO, ITEM, CODATUALIZACAO, QUAN1, VAUN1, VATO1, QTDENT, SUBEM, STATUS, ULTIMA)  
                        SELECT B.ITEM, DATEADD(1 YEAR TO A.DTHOM), B.NUMLIC, B.CODIF, B.CADPRO, B.CODCCUSTO, B.ITEM, 0, B.QUAN1, B.VAUN1, B.VATO1, 0, B.SUBEM, B.STATUS, 'S'  
                        FROM CADLIC A INNER JOIN CADPRO B ON (A.NUMLIC = B.NUMLIC) WHERE A.REGISTROPRECO = 'S' AND A.DTHOM IS NOT NULL  
                        AND NOT EXISTS(SELECT 1 FROM REGPRECO X  
                        WHERE X.NUMLIC = B.NUMLIC AND X.CODIF = B.CODIF AND X.CADPRO = B.CADPRO AND X.CODCCUSTO = B.CODCCUSTO AND X.ITEM = B.ITEM);  

                        INSERT INTO REGPRECOHIS (NUMLIC, CODIF, CADPRO, CODCCUSTO, ITEM, CODATUALIZACAO, QUAN1, VAUN1, VATO1, SUBEM, STATUS, MOTIVO, MARCA, NUMORC, ULTIMA)  
                        SELECT B.NUMLIC, B.CODIF, B.CADPRO, B.CODCCUSTO, B.ITEM, 0, B.QUAN1, B.VAUN1, B.VATO1, B.SUBEM, B.STATUS, B.MOTIVO, B.MARCA, B.NUMORC, 'S'  
                        FROM CADLIC A INNER JOIN CADPRO B ON (A.NUMLIC = B.NUMLIC) WHERE A.REGISTROPRECO = 'S' AND A.DTHOM IS NOT NULL  
                        AND NOT EXISTS(SELECT 1 FROM REGPRECOHIS X  
                        WHERE X.NUMLIC = B.NUMLIC AND X.CODIF = B.CODIF AND X.CADPRO = B.CADPRO AND X.CODCCUSTO = B.CODCCUSTO AND X.ITEM = B.ITEM);  
                    
                        insert into cadprolic_detalhe_fic (numlic, item, codigo, qtd, valor, qtdadt, valoradt, codccusto, qtdmed, valormed, tipo)
                        select numlic, item, '0', quan1, vato1, quan1, vato1, codccusto, quan1, vato1, 'C' from regpreco where numlic in 
                        (select numlic from cadlic where registropreco='S' and liberacompra='S') and subem=1;
                        END;""")
    commit()

def vincula_cotacao_licitacao(): # Vincula cotação a licitação
    consulta = fetchallmap(f"""
                            select
                                anogrupo,
                                cast(codgrupo as varchar) codgrupo,
                                convit,
                                sigla,
                                anoc,
                                'S' registropreco,
                                RIGHT('000000'+cast(convit as varchar),6)+'/'+SUBSTRING(anoc,3,2) proclic 
                            from
                                mat.MCT91200
                            where
                                anogrupo >= {ANO-5}
                                and convit is not null
                                --Agrupamento RP
                            union all
                            select
                                anogrupo,
                                cast(codgrupo as varchar) codgrupo,
                                convit,
                                sigla,
                                anoc,
                                'N' registropreco,
                                RIGHT('000000'+cast(convit as varchar),6)+'/'+SUBSTRING(anoc,3,2) proclic
                            from
                                mat.MCT80200
                            where
                                anogrupo >= {ANO-5}
                                and convit is not null
                                --Agrupamento
                            order by
                                anogrupo,
                                codgrupo
                            """)
    
    update = cur_fdb.prep('Update cadorc set numlic = ?, proclic = ? where numorc = ?')
    
    for row in tqdm(consulta, desc='Vinculando licitações x cotações...'):
        numlic = LICITACAO[(int(row['convit']), row['sigla'], row['anoc'])] #, row['registropreco']
        try:
            numorc = COTACAO[(row['codgrupo'], row['anoc'], row['registropreco'])]
            cur_fdb.execute(update,(numlic,row['proclic'],numorc))
        except:
            continue
    commit()

def aditamento(): # Aditamentos de licitações
    consulta = fetchallmap(f"""select
                                    b.sigla,
                                    b.convit,
                                    b.anoc,
                                    codfor codif,
                                    a.estrut + '.' + a.grupo + '.' + a.subgrp + '.' + a.itemat + '-' + a.digmat cadpro,
                                    sum(a.qtde) qtd,
                                    sum(a.valor) vaun,
                                    sum(a.vlrtotal) vatoadt
                                from
                                    mat.MCT66900 a
                                join mat.MCT80200 b on
                                    b.idAditivo = a.IdAditivo
                                join mat.MCT73300 c on
                                    c.IdAditivo = a.IdAditivo
                                where b.anoc >= {ANO-5}
                                GROUP by
                                    b.sigla,
                                    b.convit,
                                    b.anoc,
                                    codfor,
                                    a.estrut + '.' + a.grupo + '.' + a.subgrp + '.' + a.itemat + '-' + a.digmat""")
    
    update_cadpro = cur_fdb.prep('UPDATE cadpro SET QTDADT = QTDADT + ?, VAUNADT = VAUNADT + ?, VATOADT = VATOADT + ? WHERE numlic = ? AND codif = ? AND cadpro = ?')

    for row in tqdm(consulta, desc='Inserindo Aditamentos...'):
        qtd_aditada = row['qtd']
        vaun_aditada = row['vaun']
        vato_aditada = row['vatoadt']
        numlic = LICITACAO[(row['convit'], row['sigla'], row['anoc'])] #, 'N'
        codif = row['codif']
        cadpro = PRODUTOS[row['cadpro']]
        cur_fdb.execute(update_cadpro,(qtd_aditada, vaun_aditada, vato_aditada, numlic, codif, cadpro))
    commit()

    cur_fdb.execute(f"""insert into cadprolic_detalhe_fic (numlic, item, codigo, qtd, valor, qtdadt, valoradt, codccusto, qtdmed, valormed, tipo) 
                    select numlic, item, '0', quan1, vato1, qtdadt, vatoadt, codccusto, quan1, vato1, 'C' from cadpro where numlic in 
                    (select numlic from cadlic where registropreco='N' and liberacompra='S') and subem=1;""")
    commit()

def cadpro_saldo_ant(): # Saldo Pedido Anterior
    cur_fdb.execute('delete from cadpro_saldo_ant')
    cria_campo('alter table cadpro_saldo_ant add codif_ant varchar(10)')

    insert = cur_fdb.prep("""insert into cadpro_saldo_ant (ano, numlic, item, cadpro, qtdped, vatoped, codif_ant) values (?,?,?,?,?,?,?)""")

    consulta = fetchallmap(f"""select
                                    a.sigla,
                                    a.convit,
                                    a.anoc ano,
                                    cast(a.codfor as integer) codif,
                                    b.estrut + '.' + b.grupo + '.' + b.subgrp + '.' + b.itemat + '-' + b.digmat cadpro,
                                    sum(b.qtde) qtde , 
                                    sum(b.total) total,
                                    isnull(c.UnidOrc,
                                    0) codant
                                from
                                    mat.MCT67000 a
                                join mat.MCT66800 b on
                                    a.numint = b.numint
                                    and a.anoint = b.anoint
                                left join mat.UnidOrcamentariaW c on
                                    a.idNivel5 = c.idNivel5
                                where
                                    a.anoc BETWEEN {ANO-5} and {ANO-1}
                                GROUP by
                                    sigla,
                                    convit,
                                    anoc,
                                    a.codfor,
                                    b.estrut + '.' + b.grupo + '.' + b.subgrp + '.' + b.itemat + '-' + b.digmat,
                                    isnull(c.UnidOrc,
                                    0)
                                order by
                                    a.anoc,
                                    a.sigla,
                                    a.convit""")
    
    for row in tqdm(consulta,desc=f'Inserindo Pedidos Anteriores à {ANO}...'):
        ano = ANO-1
        numlic = LICITACAO[(row['convit'], row['sigla'], row['ano'])]
        cadpro = PRODUTOS[row['cadpro']]
        try:
            item = ITEM_PROPOSTA[numlic, cadpro, row['codif']]
        except:
            continue
        qtdped = row['qtde']
        vatoped = row['total']
        codif_ant = row['codif']
        try:
            cur_fdb.execute(insert,(ano, numlic, item, cadpro, qtdped, vatoped, codif_ant))
            commit()
        except:
            continue

def fase_iv(): # Fase IV
    consulta = fetchallmap(f"""
                            SELECT 
                                Licitacao,
                                [Valor Estimado] valor,
                                'S' enviou_tce,
                                [Codigo Tribunal] codtce
                            FROM 
                                mat.ViewLicitacoesAUDESP vla
                            where [Codigo Tribunal] <> ''
                            """)
    
    for row in tqdm(consulta, desc='Inserindo Fase V...'):
        processo = row['Licitacao'].split('-')
        mascmod = (f"{processo[0]}-{processo[-1]}").replace(' ','')
        cur_fdb.execute(f"update cadlic set codtce = {row['codtce']}, enviotce = 'S', valor = {row['valor']}  where mascmod = '{mascmod}'")
    commit()

def vinculacao_contratos(): # Vincula os Contratos
    consulta = fetchallmap(f'''select
                                c.idContrato ,
                                cast(g.sigla as varchar) +'-'+ cast(g.convit as varchar)+'/'+ cast(g.anoc as varchar) as mascmod
                            from
                                mat.MDT00100 c
                            join mat.mct80200 g on
                                g.IdAgrupamento = c.idAgrupamento
                            where g.anoc >= {ANO-5}''')
    
    for row in tqdm(consulta, desc='Inserindo Vinculação de Contratos...'):
        cur_fdb.execute(f"update contratos a set a.proclic = (select b.proclic from cadlic b where b.mascmod='{row['mascmod']}') where a.idcontratoam = {row['idContrato']}")
        commit()

        with open('Scripts/vinculacao_contratos.txt', 'a') as f:
            f.write("update contratos a set a.proclic = (select b.proclic from cadlic b where b.mascmod='{}') where a.idcontratoam = {};\n".format(row['mascmod'], row['idContrato']))

    cur_fdb.execute("UPDATE contratos a SET a.numlic = (SELECT b.nlicitacao FROM CADLICITACAO b WHERE a.proclic = b.proclic) WHERE a.proclic IS NOT NULL")
    commit()
    # print(updates)