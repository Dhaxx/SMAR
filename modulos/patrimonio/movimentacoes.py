from modulos.patrimonio import *

def aquisicao():
    cur_fdb.execute("delete from pt_movbem where tipo_mov = 'A'")
    
    codigo_mov = cur_fdb.execute('SELECT COALESCE(MAX(CODIGO_MOV), 0) FROM PT_MOVBEM').fetchone()[0]

    consulta =  cur_fdb.execute('''
                    SELECT
                        P.EMPRESA_PAT,
                        P.CODIGO_PAT,
                        P.DISCR_PAT,
                        P.DTLAN_PAT,
                        P.DATAE_PAT,
                        P.CODIGO_CPL_PAT,
                        P.CODIGO_SET_PAT,
                        P.CODIGO_SET_ATU_PAT,
                        P.VALAQU_PAT,
                        P.VALATU_PAT,
                        (P.VALAQU_PAT - P.VALATU_PAT) AS AJUSTE,
                        P.BALCO_PAT
                    FROM
                        PT_CADPAT P
                    WHERE
                        P.DATAE_PAT IS NOT NULL
                    ORDER BY
                        P.CODIGO_PAT''').fetchallmap()
    
    insert_movbem = cur_fdb.prep('''
                                    INSERT INTO
                                        pt_movbem (empresa_mov,
                                        codigo_mov,
                                        codigo_pat_mov,
                                        data_mov,
                                        tipo_mov,
                                        codigo_cpl_mov,
                                        balco_mov,
                                        codigo_set_mov,
                                        valor_mov,
                                        documento_mov,
                                        historico_mov)
                                    VALUES (?,?,?,?,?,
                                            ?,?,?,?,?,
                                            ?)
                                ''')

    for row in tqdm(consulta, desc='PATRIMÔNIO - INSERINDO AQUISIÇÕES'):
        codigo_mov += 1
        empresa_mov = row['empresa_pat']
        codigo_pat_mov = row['codigo_pat']
        data_mov = row['datae_pat']
        tipo_mov = 'A'
        codigo_cpl_mov = row['codigo_cpl_pat']
        balco_mov = row['balco_pat']
        codigo_set_mov = row['codigo_set_pat']
        valor_mov = row['valaqu_pat']
        documento_mov = 'AQUISIÇÃO'
        historico_mov = row['discr_pat']
        cur_fdb.execute(insert_movbem, (empresa_mov, codigo_mov, codigo_pat_mov, data_mov, tipo_mov, codigo_cpl_mov, balco_mov,
                                        codigo_set_mov, valor_mov, documento_mov, historico_mov))
    commit()

def ajuste():
    cur_fdb.execute("delete from pt_movbem where tipo_mov = 'R' and depreciacao_mov = 'N'")
    
    codigo_mov = cur_fdb.execute('SELECT COALESCE(MAX(CODIGO_MOV), 0) FROM PT_MOVBEM').fetchone()[0]

    consulta =  cur_fdb.execute('''
                    SELECT
                        P.EMPRESA_PAT,
                        P.CODIGO_PAT,
                        P.DISCR_PAT,
                        P.DTLAN_PAT,
                        P.DATAE_PAT,
                        P.CODIGO_CPL_PAT,
                        P.CODIGO_SET_PAT,
                        P.CODIGO_SET_ATU_PAT,
                        P.VALAQU_PAT,
                        P.VALATU_PAT,
                        -(P.VALAQU_PAT - P.VALATU_PAT) AS AJUSTE,
                        P.BALCO_PAT
                    FROM
                        PT_CADPAT P
                    WHERE
                        P.DATAE_PAT IS NOT NULL
                    ORDER BY
                        P.CODIGO_PAT''').fetchallmap()
    
    insert_movbem = cur_fdb.prep('''
                                    INSERT INTO
                                        pt_movbem (empresa_mov,
                                        codigo_mov,
                                        codigo_pat_mov,
                                        data_mov,
                                        tipo_mov,
                                        incorpora_mov,
                                        depreciacao_mov,
                                        codigo_set_mov,
                                        valor_mov,
                                        documento_mov,
                                        historico_mov)
                                    VALUES (?,?,?,?,?,
                                            ?,?,?,?,?,
                                            ?)
                                ''')

    for row in tqdm(consulta, desc='PATRIMÔNIO - INSERINDO AJUSTES'):
        codigo_mov += 1
        empresa_mov = row['empresa_pat']
        codigo_pat_mov = row['codigo_pat']
        data_mov = row['datae_pat']
        tipo_mov = 'R'
        incorpora_mov = 'N'
        depreciacao_mov = 'S'
        codigo_set_mov = row['codigo_set_pat']
        valor_mov = row['ajuste']
        documento_mov = 'AJUSTE'
        historico_mov = row['discr_pat']
        cur_fdb.execute(insert_movbem, (empresa_mov, codigo_mov, codigo_pat_mov, data_mov, tipo_mov, incorpora_mov, depreciacao_mov,
                                        codigo_set_mov, valor_mov, documento_mov, historico_mov))
    commit()

def baixas():
    cur_fdb.execute("delete from pt_movbem where tipo_mov = 'B'")
    
    codigo_mov = cur_fdb.execute('SELECT COALESCE(MAX(CODIGO_MOV), 0) FROM PT_MOVBEM').fetchone()[0]

    consulta = cur_fdb.execute('''SELECT P.EMPRESA_PAT AS EMPRESA_MOV, P.CODIGO_PAT AS CODIGO_PAT_MOV, P.DTPAG_PAT AS DATA_MOV, P.CODIGO_BAI_PAT AS CODIGO_BAI_MOV,
    P.CODIGO_SET_ATU_PAT AS CODIGO_SET_MOV, P.VALATU_PAT AS VALOR_MOV, NULL AS HISTORICO_MOV FROM PT_CADPAT P WHERE P.DTPAG_PAT IS NOT NULL
    ORDER BY P.CODIGO_PAT''').fetchallmap()

    insert_movbem = cur_fdb.prep('''
                                INSERT INTO
                                    pt_movbem (empresa_mov,
                                    codigo_mov,
                                    codigo_pat_mov,
                                    data_mov,
                                    codigo_bai_mov,
                                    tipo_mov,
                                    incorpora_mov,
                                    depreciacao_mov,
                                    codigo_set_mov,
                                    valor_mov,
                                    documento_mov,
                                    historico_mov)
                                VALUES (?,?,?,?,?,
                                        ?,?,?,?,?,?,
                                        ?)
                                 ''')

    for row in tqdm(consulta, desc='PATRIMÔNIO - INSERINDO BAIXAS'):
        codigo_mov += 1
        empresa_mov = row['empresa_mov']
        codigo_pat_mov = row['codigo_pat_mov']
        data_mov = row['data_mov']
        codigo_bai_mov = row['codigo_bai_mov']
        tipo_mov = 'B'
        incorpora_mov = 'N'
        depreciacao_mov = 'N'
        codigo_set_mov = row['codigo_set_mov']
        valor_mov = row['valor_mov']*-1
        documento_mov = 'BAIXA'
        historico_mov = row['historico_mov']
        cur_fdb.execute(insert_movbem, (empresa_mov, codigo_mov, codigo_pat_mov, data_mov, codigo_bai_mov, tipo_mov, incorpora_mov, depreciacao_mov,
                                        codigo_set_mov, valor_mov, documento_mov, historico_mov))
    # cur_fdb.execute('''
    #                 merge into pt_cadpat a using pt_movbem b on (a.codigo_pat = b.codigo_pat_mov and b.tipo_mov = 'B')
    #                 when matched then update set a.dtpag_pat = b.data_mov, a.codigo_bai_pat = coalesce(b.codigo_bai_mov, 0)
    #                 ''')
    # cur_fdb.execute('''
    #                 UPDATE PT_CADPAT A SET
    #                 A.CODIGO_SET_ATU_PAT = (SELECT FIRST 1 X.CODIGO_SET_MOV FROM PT_MOVBEM X
    #                 WHERE A.CODIGO_PAT = X.CODIGO_PAT_MOV AND X.TIPO_MOV = 'T' AND X.CODIGO_SET_MOV IS NOT NULL
    #                 ORDER BY X.CODIGO_MOV DESC)
    #                 WHERE EXISTS(SELECT 1 FROM PT_MOVBEM X WHERE A.CODIGO_PAT = X.CODIGO_PAT_MOV
    #                 AND X.TIPO_MOV = 'T')
    #                 ''')
    cur_fdb.execute("update pt_cadpat set valatu_pat = 0 where dtpag_pat is not null")
    commit()

def depreciacoes():
    cur_fdb.execute("delete from pt_movbem where depreciacao_mov = 'S'")

    insert = cur_fdb.prep('''
                            INSERT INTO
                                pt_movbem (codigo_mov,
                                empresa_mov,
                                codigo_pat_mov,
                                data_mov,
                                tipo_mov,
                                codigo_cpl_mov,
                                codigo_set_mov,
                                valor_mov,
                                historico_mov,
                                lote_mov,
                                percentual_mov,
                                depreciacao_mov)
                            values (?,?,?,?,?,?,?,?,?,?,?,?)
                         ''')
    
    consulta = fetchallmap('''
                                select
                                t808.IdPatrimonio,
                                t808.dtcalculo,
                                'R' tipo_mov,
                                nivel1 + '.' + nivel2 + '.' + nivel3 + '.' + nivel4 + '.' + nivel5 codigo_set_mov,
                                -vlrcotadep valor_mov,
                                perctxdep percentual_mov,
                                'S' depreciacao_mov,
                                'DEPRECIAÇÃO - ' + CAST(t808.mes as varchar)+ '/' + cast(t808.ano as varchar) historico_mov
                            from
                                mat.MPT80800 t808
                            left join mat.MXT71100 t711 on
                                t808.idunidorc = t711.IdNivel5
                            order by
                                IdPatrimonio,
                                t808.ano,
                                t808.mes
    ''')

    codigo_mov = cur_fdb.execute('SELECT COALESCE(MAX(CODIGO_MOV), 0) FROM PT_MOVBEM').fetchone()[0]

    for row in tqdm(consulta, desc='PATRIMÔNIO - DEPRECIAÇÕES'):
        codigo_mov += 1
        empresa_mov = EMPRESA
        codigo_pat_mov = row['codigo_pat_mov']
        data_mov = row['dtcalculo']
        tipos_mov = row['tipo_mov']
        codigo_cpl_mov = None
        codigo_set_mov = 0
        valor_mov = row['valor_mov']
        historico_mov = row['historico_mov']
        lote_mov = None
        percentual_mov = row['percentual_mov']
        depreciacao_mov = row['depreciacao_mov']

        cur_fdb.execute(insert, (codigo_mov, empresa_mov, codigo_pat_mov, data_mov, tipos_mov, codigo_cpl_mov, codigo_set_mov, 
                                 valor_mov, historico_mov, lote_mov, percentual_mov, depreciacao_mov))
    commit()