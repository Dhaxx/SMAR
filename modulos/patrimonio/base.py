from modulos.patrimonio import *

def tipos_mov():
    cur_fdb.execute('delete from pt_tipomov')
    
    valores = [
        ("A", "AQUISIÇÃO"),
        ("B", "BAIXA"),
        ("T", "TRANSFERÊNCIA"),
        ("R", "PR. CONTÁBIL"),
        ("P", "TRANS. PLANO")]
    
    cur_fdb.executemany('insert into pt_tipomov (codigo_tmv, descricao_tmv) values (?, ?)', valores)
    commit()

def tipos_ajuste():
    cur_fdb.execute("""delete from pt_cadajuste""")
    insert = cur_fdb.prep("INSERT INTO PT_CADAJUSTE (CODIGO_AJU, EMPRESA_AJU, DESCRICAO_AJU) VALUES (?, ?, ?)")
    
    valores = [(1, EMPRESA, "REAVALIAÇÃO(ANTES DO CORTE)")]

    cur_fdb.executemany(insert, valores)
    commit()

def tipos_baixa():
    cur_fdb.execute('delete from pt_cadbai')
    cria_campo('alter table pt_cadbai add codsis_ant varchar(20)')
    cria_campo('alter table pt_cadbai add codtab_ant varchar(20)')
    cria_campo('alter table pt_cadbai add codite_ant varchar(20)')
    
    consulta = fetchallmap('''
        SELECT
            a.codsis,
            a.codtab,
            a.codite,
            a.descrc
        FROM
            mat.MXT60500 A
        WHERE
            a.codsis = '003'
            AND a.codtab = '014'
            AND A.CODITE <> '00'
        ORDER BY
            1,
            2
    ''')

    insert = cur_fdb.prep("INSERT INTO PT_CADBAI (CODIGO_BAI, EMPRESA_BAI, DESCRICAO_BAI, CODSIS_ANT, CODTAB_ANT, CODITE_ANT) VALUES (?, ?, ?, ?, ?, ?)")

    i = 0

    for row in tqdm(consulta, desc="PATRIMÔNIO - Tipos de Baixa"):
        i += 1
        codigo_bai = i
        empresa_bai = EMPRESA
        descricao_bai = row['descrc']
        codsis_ant = row['codsis']
        codtab_ant = row['codtab']
        codite_ant = row['codite']
        valores = (codigo_bai, empresa_bai, descricao_bai, codsis_ant, codtab_ant, codite_ant)
        cur_fdb.execute(insert, valores)
    commit()

def tipos_bens():
    cur_fdb.execute('delete from pt_cadtip')

    cria_campo('alter table pt_cadtip add ID_IDCLSPATRIMONIAL varchar(20)')
    cria_campo('alter table pt_cadtip add ID_GRPBENS varchar(20)')
    cria_campo('alter table pt_cadtip add ID_CDCLASSE varchar(20)')
    cria_campo('alter table pt_cadtip add ID_ICTIPCADASTRO varchar(20)')
    cria_campo('alter table pt_cadtip add percent integer')
    cria_campo('alter table pt_cadtip add meses integer')

    consulta = fetchallmap('''
                                SELECT
                                    a.idclspatrimonial,
                                    a.grpbens,
                                    a.cdclasse,
                                    substring(a.dcclspatrimonial, 1, 60) dcclspatrimonial ,
                                    A.ictipcadastro,
                                    percvlrresidual,
                                    anosvidautil * 12 meses
                                FROM
                                    MAT.MPT05000 A
                                ORDER BY
                                    1,
                                    2,
                                    3
    ''')

    insert = cur_fdb.prep("""insert into pt_cadtip (codigo_tip, empresa_tip, descricao_tip, id_idclspatrimonial, 
                             id_grpbens, id_cdclasse, id_ictipcadastro, codigo_tce_tip, percent, meses) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")
    codigo_tip = 0

    for row in tqdm(consulta, desc="PATRIMÔNIO - Tipos de Bens"):
        codigo_tip += 1
        empresa_tip = EMPRESA
        descricao_tip = row['dcclspatrimonial']
        id_idclspatrimonial = row['idclspatrimonial']
        id_grpbens = row['grpbens']
        id_cdclasse = row['cdclasse']
        id_ictipcadastro = row['ictipcadastro']
        codigo_tce_tip = cur_fdb.execute(f"select balco from conpla_tce where titco containing '{row['dcclspatrimonial']}' and (balco starting '123' or balco starting '331')").fetchone()
        codigo_tce_tip = codigo_tce_tip[0] if codigo_tce_tip else None
        percent = row['percvlrresidual']
        meses = row['meses']
        valores = (codigo_tip, empresa_tip, descricao_tip, id_idclspatrimonial, id_grpbens, id_cdclasse, id_ictipcadastro, codigo_tce_tip, percent, meses)
        cur_fdb.execute(insert, valores)
    commit()

def tipos_situacao():
    cur_fdb.execute('delete from pt_cadsit')   
    print('PATRIMÔNIO - Inserindo Situações')

    cria_campo('alter table pt_cadsit add id_codsis varchar(20)')
    cria_campo('alter table pt_cadsit add id_codtab varchar(20)')
    cria_campo('alter table pt_cadsit add id_codite varchar(20)')   

    valores = [
        ("1", EMPRESA, "EXCELENTE"),
        ("2", EMPRESA, "BOM"),
        ("3", EMPRESA, "REGULAR"),
        ("4", EMPRESA, "PÉSSIMO")]

    insert = cur_fdb.prep("INSERT INTO PT_CADSIT (CODIGO_SIT, EMPRESA_SIT, DESCRICAO_SIT) VALUES (?, ?, ?)")

    cur_fdb.executemany(insert, valores)
    commit()

def grupos():
    cur_fdb.execute('delete from pt_cadpatg')        

    insert = cur_fdb.prep("INSERT INTO PT_CADPATG (CODIGO_GRU, EMPRESA_GRU, NOGRU_GRU) VALUES (?, ?, ?)")
    valores = [(1, EMPRESA, 'Móveis'),
               (2, EMPRESA, 'Veículos'),
               (3, EMPRESA, 'Imóveis'),
               (4, EMPRESA, 'Acervo')]

    cur_fdb.executemany(insert, valores)
    commit()
        
def unidade_subunidade():
    cur_fdb.execute('delete from pt_cadpats')
    cur_fdb.execute('delete from pt_cadpatd')
    print('PATRIMÔNIO - Unidade/Subunidade')
    cria_campo('alter table pt_cadpatd add pkant varchar(20)')
    cria_campo('alter table pt_cadpats add pkant varchar(20)')

    cur_fdb.execute(f'''
                    insert into pt_cadpatd (codigo_des, empresa_des, nauni_des, ocultar_des, pkant)
                    select codccusto, empresa, descr, ocultar, cod_ant from centrocusto 
                    ''')
    cur_fdb.execute(f'''
                    insert into pt_cadpats (codigo_set, empresa_set, codigo_des_set, noset_set, ocultar_set, pkant)
                    select codccusto, empresa, codccusto, descr, ocultar, cod_ant from centrocusto
                    ''')
    commit()