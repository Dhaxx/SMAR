from conexao import cur_fdb, commit, fetchallmap
from tqdm import tqdm
import re

ANO = int(cur_fdb.execute("SELECT mexer FROM cadcli").fetchone()[0])
EMPRESA = cur_fdb.execute("SELECT empresa FROM cadcli").fetchone()[0]

def fornecedores_smar():
    hash_map = {}

    consulta = fetchallmap("select distinct rtrim(documento) documento, razao_social from mat.MCT81800")

    for row in consulta:
        hash_map[row['documento']] = row['razao_social']
    return hash_map
FORNECEDORES_SMAR = fornecedores_smar()

def extourou_codigo_item(grupo, subgrupo):
    return cur_fdb.execute(
            f"select nome from cadsubgr where grupo = '{grupo}' and subgrupo_ant = '{subgrupo}'").fetchone()[0]

def get_fetchone(sql):
    return cur_fdb.execute(sql).fetchone()[0]

def cria_campo(query):
    try:
        cur_fdb.execute(query)
        commit()
    except:
        pass

def produtos():
    cria_campo('ALTER TABLE cadest ADD cod_ant varchar(14)')
    hash_map = {}

    cur_fdb.execute("select cadpro, COD_ANT from cadest")

    for row in cur_fdb.fetchallmap():
        hash_map[row['cod_ant']] = row['cadpro']

    return hash_map

def lista_cotacoes():
    cur_fdb.execute("select numorc, ano, obs, registropreco from cadorc where obs starting 'Agrupamento'")

    hash_map = {}

    for row in cur_fdb.fetchallmap():
        agrupamento = re.search(r'\b(\d+)\b',row['obs'])
        hash_map[(agrupamento.group(1),row['ano'],row['registropreco'])] = row['numorc']
    return hash_map

def licitacoes():
    cria_campo('ALTER TABLE CADLIC ADD criterio_ant varchar(30)')
    cria_campo('ALTER TABLE CADLIC ADD sigla_ant varchar(2)')
    cria_campo('ALTER TABLE CADLIC ADD status_ant varchar(1)')

    cur_fdb.execute("select numpro, sigla_ant, ano, registropreco, numlic from cadlic")

    hash_map = {}

    for row in cur_fdb.fetchallmap():
        hash_map[(row['numpro'], row['sigla_ant'], row['ano'])] = row['numlic'] # row['registropreco']
    return hash_map

def fornecedores():
    cur_fdb.execute('select nome, codif from desfor')

    hash_map_nome = {}
    hash_map_insmf = {}

    for row in cur_fdb.fetchallmap():
        hash_map_nome[int(row['codif'])] = row['nome'][:40]

    cur_fdb.execute('select distinct(insmf), codif from desfor where insmf is not null')

    for row in cur_fdb.fetchallmap():
        hash_map_insmf[row['insmf']] = int(row['codif'])

    return hash_map_nome, hash_map_insmf

def cadastra_fornecedor_especifico(insmf, codfor):
    insert = cur_fdb.prep('insert into desfor (codif, nome, insmf) values (?,?,?)')

    if insmf and codfor:
        verifica = cur_fdb.execute(f"select codif, nome from desfor where insmf containing '{insmf}' or codif = {codfor}").fetchone()
    elif insmf:
        verifica = cur_fdb.execute(f"select codif, nome from desfor where insmf containing '{insmf}'").fetchone()
    else:
        verifica = cur_fdb.execute(f"select codif, nome from desfor where codif containing '{codfor}'").fetchone()

    if not verifica:
        codif = cur_fdb.execute('select max(codif)+1 from desfor').fetchone()[0]
        nome = FORNECEDORES_SMAR.get(insmf,'Verificar Fornecedor {}'.format(insmf)) #'Verificar Fornecedor {}'.format(insmf)
        cur_fdb.execute(insert,(codif, nome[:50], insmf))
        commit()
        return codif, nome
    else:
        return verifica[0], verifica[1]

def fornecedores_gerais():
    nome, insmf = fornecedores()
    consulta = fetchallmap('''select
                                    distinct *
                                from
                                    (
                                    select
                                        RTRIM(a.codfor) codfor,
                                        rtrim(b.desnom) desnom,
                                        rtrim(b.dcto01) insmf
                                    from
                                        mat.MXT60100 a
                                    join mat.MXT61400 b on
                                        a.codnom = b.codnom
                                union all
                                    select
                                        case
                                            when m.codfor = ''
                                            or m.codfor is null then 797979
                                            else m.codfor
                                        end codfor,
                                            rtrim(m.descricao) desnom,
                                            rtrim(m.documento) insmf
                                    from
                                            mat.MCT81800 m) as query
                                order by
                                    codfor ASC''')
    insert = cur_fdb.prep('insert into desfor (codif, nome, insmf, codif_ant) values (?,?,?,?)')
    
    codif = cur_fdb.execute('select max(codif) from desfor').fetchone()[0]

    for row in tqdm(consulta, desc='Inserindo Fornecedores Faltantes'):
        verifica = nome.get(row['codfor'], insmf.get(row['insmf'], None))
        if not verifica:    
            codif += 1
            cur_fdb.execute(insert, (codif, row['desnom'], row['insmf'], row['codfor']))
            nome[row['codfor']] = row['desnom']
            insmf[row['insmf']] = codif
    commit()

def ajustar_ccusto_cotacao():
    print('Ajustando Centro de custos da cotação...')
    hash_map = {}
    update_cadorc = cur_fdb.prep('update cadorc set codccusto = (select codccusto from centrocusto where cod_ant = ?) where codccusto = ?')
    update_icadorc = cur_fdb.prep('update icadorc set codccusto = (select codccusto from centrocusto where cod_ant = ?) where codccusto = ?')

    consulta = fetchallmap("""select IdNivel5, nivel1 + '.' + nivel2 + '.' + nivel3 +  + '.' + nivel4 + '.' + nivel5 as codant from mat.MXT71100 m""")
    for row in consulta:
        hash_map[row['IdNivel5']] = row['codant']

    cur_fdb.execute('select distinct codccusto from cadorc where codccusto <> 0')
    for row in tqdm(cur_fdb.fetchallmap()):
        cod_ant = hash_map[row['codccusto']]
        cur_fdb.execute(update_cadorc, (cod_ant,row['codccusto']))
        cur_fdb.execute(update_icadorc, (cod_ant,row['codccusto']))
    commit()

def depara_ccusto():
    cria_campo("alter table centrocusto add cod_ant varchar(19)")
    hash_map = {}
    cur_fdb.execute('select cod_ant, codccusto from centrocusto')

    for row in cur_fdb.fetchallmap():
        hash_map[row['cod_ant']] = row['codccusto']
    return hash_map

def item_da_proposta():
    cria_campo("alter table cadpro_final add CQTDADT double precision")
    cria_campo("alter table cadpro_final add ccadpro varchar(20)")
    cria_campo("alter table cadpro_final add CCODCCUSTO integer;")
    hash_map = {}

    cur_fdb.execute('select numlic, ccadpro, codif, itemp from cadpro_final')

    for row in cur_fdb.fetchallmap():
        hash_map[(row['numlic'], row['ccadpro'], row['codif'])] = row['itemp']
    return hash_map

def veiculo_tipo():
    hash_map = {}

    cur_fdb.execute('select codigo_tip, descricao_tip from veiculo_tipo')

    for row in cur_fdb.fetchallmap():
        hash_map[row['descricao_tip']] = row['codigo_tip']
    return hash_map

def veiculo_marca():
    hash_map = {}

    cur_fdb.execute('SELECT CODIGO_MAR, DESCRICAO_MAR FROM VEICULO_MARCA vm')

    for row in cur_fdb.fetchallmap():
        hash_map[row['descricao_mar']] = row['codigo_mar']
    return hash_map

def unidades():
    cria_campo('alter table pt_cadpatd add pkant varchar(20)')
    hash_map = {}

    cur_fdb.execute('select pkant, codigo_des from pt_cadpatd')

    for row in cur_fdb.fetchallmap():
        hash_map[row['pkant']] = row['codigo_des']
    return hash_map

def subunidades():
    cria_campo('alter table pt_cadpats add pkant varchar(20)')
    hash_map = {}

    cur_fdb.execute('select pkant, codigo_set from pt_cadpats')

    for row in cur_fdb.fetchallmap():
        hash_map[row['pkant']] = row['codigo_set']
    return hash_map

def plano_contas():
    hash_map = {}

    cur_fdb.execute('select titco, balco from conpla_tce')

    for row in cur_fdb.fetchallmap():
        hash_map[row['titco']] = row['balco']
    return hash_map
    
def lista_contratos():
    hash_map = {}

    cur_fdb.execute('select idcontratoam, codigo from contratos')

    for row in cur_fdb.fetchallmap():
        hash_map[row['idcontratoam']] = row['codigo']
    return hash_map

def aditivos_contratos():
    cur_fdb.execute("delete from contratosaditamento")
    contratos = lista_contratos()
    cmds = []
    
    consulta = fetchallmap("""select
                                    cast(idContrato as varchar) idContrato,
                                    RIGHT('00000' + numeroAlteracao,
                                    4)+ '/' + SUBSTRING(cast(anoAlteracao as varchar), 3, 4) termo,
                                    isnull(dataInicioVigencia, dataAssinatura) dtlan,
                                    justificativaAlteracao descricao,
                                    dataTerminoVigencia dataencerramento,
                                    'OUTROS' veic_publicacao,
                                    null tipo_tce,
                                    valorAditivo valor,
                                    case when valorAditivo > 0 then 'Acréscimo' else 'Decréscimo' end tipohist,
                                    dataAssinatura dtinsc
                                from
                                    mat.MDT03000 m
                                """)

    insert = cur_fdb.prep("""INSERT
                                INTO
                                contratosaditamento(contrato,
                                termo,
                                dtlan,
                                descricao,
                                dataencerramento,
                                dtpublicacao,
                                veic_publicacao,
                                tipo_tce,
                                valor,
                                tipohist,
                                datainsc,
                                dataabertura,
                                dtautoriz,
                                possuiautoriztermo,
                                codigo)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""")
    
    codigo = 0

    for row in tqdm(consulta, desc='Inserindo Aditivos de Contrato'):
        contrato = contratos.get(row['idContrato'], None)
        if contrato:
            termo = row['termo']
            dtlan = row['dtlan']
            descricao = row['descricao']
            dataencerramento = row['dataencerramento']
            dtpublicacao = row['dtlan']
            veic_publicacao = row['veic_publicacao']
            tipo_tce = row['tipo_tce']
            valor = row['valor']
            tipohist = row['tipohist']
            datainsc = row['dtinsc']
            dataabertura = row['dtlan']
            dtautoriz = row['dtlan']
            possuiautoriztermo = None
            codigo += 1
            cur_fdb.execute(insert, (contrato, termo, dtlan, descricao, dataencerramento, dtpublicacao, veic_publicacao, tipo_tce, valor, tipohist, 
                                    datainsc, dataabertura, dtautoriz, possuiautoriztermo, codigo))
            cmds.append(f"""INSERT
                                INTO
                                contratosaditamento(contrato,
                                termo,
                                dtlan,
                                descricao,
                                dataencerramento,
                                dtpublicacao,
                                veic_publicacao,
                                tipo_tce,
                                valor,
                                tipohist,
                                datainsc,
                                dataabertura,
                                dtautoriz,
                                possuiautoriztermo,
                                codigo)
                            VALUES ({contrato, termo, dtlan, descricao, dataencerramento, dtpublicacao, veic_publicacao, tipo_tce, valor, tipohist, 
                                    datainsc, dataabertura, dtautoriz, possuiautoriztermo, codigo});""")
    commit()
    with open('output.txt', 'w') as f:
        for cmd in cmds:
            f.write("%s\n" % cmd)

def insere_cadpro_cadped():
    filtro = []
    
    for row in filtro:
        cur_fdb.execute(f"""INSERT
                                INTO
                                cadpro (codif,
                                cadpro,
                                quan1,
                                vaun1,
                                vato1,
                                subem,
                                status,
                                item,
                                itemorcped,
                                codccusto,
                                numlic,
                                ult_sessao,
                                itemp,
                                qtdadt,
                                qtdped,
                                vaunadt,
                                vatoadt,
                                tpcontrole_saldo)
                                SELECT
                                c.CODIF,
                                i.CADPRO,
                                SUM(i.QTD) AS qtd,
                                i.PRCUNT,
                                SUM(i.PRCTOT) AS total_price,
                                1 AS subem,
                                'C' AS classificado,
                                i.item,
                                i.item,
                                i.CODCCUSTO,
                                c.NUMLIC,
                                1 AS ult_sessao,
                                i.ITEM,
                                i.QTD,
                                SUM(i.QTD) AS qtd_sum,
                                i.PRCUNT,
                                SUM(i.PRCTOT) AS total_price_sum,
                                'Q' AS tpcontrole_saldo
                            FROM
                                ICADPED i
                            JOIN
                                cadped c ON
                                i.ID_CADPED = c.ID_CADPED
                            WHERE
                                mascmod_ant = '{row[0]}'
                            GROUP BY
                                c.CODIF,
                                i.CADPRO,
                                i.PRCUNT,
                                subem,
                                classificado,
                                i.item,
                                i.item,
                                i.CODCCUSTO,
                                c.NUMLIC,
                                ult_sessao,
                                i.ITEM,
                                i.QTD,
                                i.PRCUNT,
                                tpcontrole_saldo""")
        
        with open('Scripts/insere_cadpro.txt', 'a') as f:
            f.write(f"""INSERT
                                INTO
                                cadpro (codif,
                                cadpro,
                                quan1,
                                vaun1,
                                vato1,
                                subem,
                                status,
                                item,
                                itemorcped,
                                codccusto,
                                numlic,
                                ult_sessao,
                                itemp,
                                qtdadt,
                                qtdped,
                                vaunadt,
                                vatoadt,
                                tpcontrole_saldo)
                                SELECT
                                c.CODIF,
                                i.CADPRO,
                                SUM(i.QTD) AS qtd,
                                i.PRCUNT,
                                SUM(i.PRCTOT) AS total_price,
                                1 AS subem,
                                'C' AS classificado,
                                i.item,
                                i.item,
                                i.CODCCUSTO,
                                c.NUMLIC,
                                1 AS ult_sessao,
                                i.ITEM,
                                i.QTD,
                                SUM(i.QTD) AS qtd_sum,
                                i.PRCUNT,
                                SUM(i.PRCTOT) AS total_price_sum,
                                'Q' AS tpcontrole_saldo
                            FROM
                                ICADPED i
                            JOIN
                                cadped c ON
                                i.ID_CADPED = c.ID_CADPED
                            WHERE
                                mascmod_ant = '{row}'
                            GROUP BY
                                c.CODIF,
                                i.CADPRO,
                                i.PRCUNT,
                                subem,
                                classificado,
                                i.item,
                                i.item,
                                i.CODCCUSTO,
                                c.NUMLIC,
                                ult_sessao,
                                i.ITEM,
                                i.QTD,
                                i.PRCUNT,
                                tpcontrole_saldo;\n""")
    commit()

def insere_crc():
    _, INSMF_FORNECEDOR = fornecedores()

    cabecalho = fetchallmap("""SELECT
                                    nro_crc,
                                    MAX(ano_crc) AS maior_ano,
                                    a.codfor,
                                    dt_emissao_orig,
                                    dt_atualizacao,
                                    dt_vencimento,
                                    rtrim(c.dcto01) insmf
                                FROM
                                    mat.MXT73700 a
                                join mat.MXT60100 b on
                                    b.codfor = a.codfor
                                left join mat.MXT61400 c on
                                    b.codnom = c.codnom
                                where c.dcto01 is not null
                                GROUP BY
                                    a.codfor,
                                    nro_crc,
                                    dt_emissao_orig,
                                    dt_atualizacao,
                                    dt_vencimento,
                                    rtrim(c.dcto01)
                                order by codfor, [maior_ano]""")
    
    documentos = fetchallmap("""select
                                    a.IdJurFis codcrc,
                                    a.codfor,
                                    b.descrc,
                                    a.datexp,
                                    a.datven,
                                    case when (a.datven) is not null then 'N' else 'S' end vencimento,
                                    rtrim(e.dcto01) insmf
                                from
                                    mat.MXT60400 a
                                join mat.MXT60500 b on
                                    a.codobr = b.codite
                                join mat.MXT60100 c on
                                    c.codfor = a.codfor
                                left join mat.MXT61400 e on
                                    c.codnom = e.codnom
                                where
                                    b.codsis = 001
                                    and b.codtab = 014
                                    and rtrim(e.dcto01) is not null
                                order by datven  --RELAÇÃO DE DOCUMENTOS""")
    
    update = cur_fdb.prep("update desfor set crcinsc = '?', crc_dtinsc = '?', crc_dtvalid = '?' where insmf = '?'")
    insert = cur_fdb.prep("insert into desforcrc (codcrc, desccertidao, datafim, codif, dataemicrc, docnumcrc, semvalidade) values (?,?,?,?,?,?,?)")
    
    for row in tqdm(cabecalho, desc='Inserindo CRCs'):
        cur_fdb.execute("update desfor set crcinsc = ?, crc_dtinsc = ?, crc_dtvalid = ? where insmf = ?", (row['nro_crc'], row['dt_emissao_orig'], row['dt_vencimento'], row['insmf']))
    commit()

    cur_fdb.execute('delete from desforcrc')

    for row in tqdm(documentos, desc='Inserindo Documentos'):
        cur_fdb.execute(insert, (row['codcrc'], row['descrc'], row['datven'], INSMF_FORNECEDOR.get(row['insmf'], 0), row['datexp'], row['insmf'], row['vencimento']))
    commit()

def insere_socios_administradores():
    _, INSMF_FORNECEDOR = fornecedores()
    codifs = []

    consulta = fetchallmap("""select
                                a.codnom,
                                SUBSTRING(b.desnom, 1, 50) desnom,
                                rtrim(b.dcto01) insmf,
                                SUBSTRING(b.razao_social, 1, 50) razao_social
                            from
                                mat.MXT61600 a
                            join mat.MXT61400 b on
                                b.codnom = a.codnom
                            where fisjur = 0""")
    
    max_codif = 63066
    
    for row in tqdm(consulta, desc='Inserindo Sócios e Administradores'):
        if INSMF_FORNECEDOR.get(row['insmf'], None):
            continue
        else:
            max_codif += 1
            cur_fdb.execute("insert into desfor (codif, nome, insmf, nom_fant) values (?,?,?,?)", (max_codif, row['desnom'], row['insmf'], row['razao_social']))
            commit()
            codifs.append(max_codif)
    print(codifs)

def datas_cadlic():
    consulta = fetchallmap(f"""SELECT
                                    LTRIM(ISNULL(query.cpcpro, e.cpcpro)) AS processo,
                                    isnull(query.cpcano, e.cpcano) ano,
                                    [datae],
                                    [dtenv],
                                    [dtrealiz],
                                    [dtadj],
                                    [dthom],
                                    [mascmod]
                                from
                                    (
                                    select
                                        a.cpcpro,
                                        a.cpcano,
                                        isnull(a.dtAbertProcAdm, b.datac) datae,
                                        b.dtAtaAberturaProposta dtenv,
                                        isnull(b.dtAbertLicitacao, b.dataadjudicacao) dtrealiz,
                                        b.dataadjudicacao dtadj,
                                        b.dataadjudicacao dthom,
                                        a.sigla + '-' + cast(a.convit as varchar)+ '/' + a.anoc mascmod
                                    from
                                        mat.MCT90300 a
                                    left join mat.MCT91400 b on
                                        a.sigla = b.sigla
                                        and a.convit = b.convit
                                        and a.anoc = b.anoc
                                    where
                                        a.anoc >= {ANO-5}
                                    UNION
                                    select
                                        NULL,
                                        NULL,
                                        isnull(c.cpcdtai, d.datac) datae,
                                        d.dtAtaAberturaProposta,
                                        d.dtAbertLicitacao dtrealiz,
                                        d.dataadjudicacao dtadj,
                                        d.dataadjudicacao dthom,
                                        c.sigla + '-' + cast(c.convit as varchar)+ '/' + c.anoc mascmod
                                    from
                                        mat.MCT69700 c
                                    left join mat.MCT67600 d on
                                        c.sigla = d.sigla
                                        and c.convit = d.convit
                                        and c.anoc = d.anoc
                                    where
                                        c.anoc >= {ANO-5}) as query
                                left JOIN mat.MCT80600 e on
                                    query.[mascmod] = e.sigla + '-' + cast(e.convit as varchar)+ '/' + e.anoc""")
    
    update = cur_fdb.prep("update cadlic set processo = ?, processo_ano = ?, datae = '?', dtenv = '?', dtreal = '?', dtadj = '?', dthom = '?' where mascmod = '?'")
    i = 0

    for row in tqdm(consulta, desc='Inserindo Datas Cadlic'):
        i += 1
        processo = row['processo']
        ano = row['ano']
        datae = row['datae']
        dtenv = row['dtenv']
        dtreal = row['dtrealiz']
        dtadj = row['dtadj']
        dthom = row['dthom']
        mascmod = row['mascmod']
        cur_fdb.execute(update, (processo, ano, datae, dtenv, dtreal, dtadj, dthom, mascmod))
    commit()

def ajusta_descritivo_cotacoes():
    consulta = fetchallmap(r"""	select
		d.idcotacao,
		c.codgrupo,
		c.anogrupo,
		replace(b.motdev, '\r\n', '') motdev
	from
		mat.MCT71900 a
	join mat.MCT90000 b on
		a.numreq = b.numreg
		and a.anoreq = b.anoreg
	join mat.MCT80200 c on
		c.IdAgrupamento = a.IdAgrupamento
	join mat.MCT79900 d on
		d.codgrupo = c.codgrupo
		and d.anogrupo = c.anogrupo
	where
		b.motdev is not null
		and b.motdev <> ''
union all
	select
		d.idcotacao,
		c.codgrupo,
		c.anogrupo,
		replace(b.motdev, '\r\n', '') motdev
	from
		mat.MCT71900 a
	join mat.MCT63400 b on
		a.numreq = b.numreq
		and a.anoreq = b.anoreq
	join mat.MCT80200 c on
		c.IdAgrupamento = a.IdAgrupamento
	join mat.MCT79900 d on
		d.codgrupo = c.codgrupo
		and d.anogrupo = c.anogrupo
	where
		b.motdev is not null
		and b.motdev <> ''""")
    
    non_printable_regex = re.compile(r'[\x00-\x1F\x7F-\x9F]')

    for row in tqdm(consulta, desc='Ajustando Descritivo Cotacoes'):
        descr = row['motdev'].replace("'", '')[:1024]
        
        # Remove todos os caracteres não imprimíveis da string
        descr = non_printable_regex.sub('', descr)
        
        cur_fdb.execute(f"""update cadorc set descr = '{descr}' where idant = {row['idcotacao']}""")
    commit()

def vincula_socio_fornecedor():
    cria_campo('alter table desfor_socios add cargo_ant varchar(50)')

    _, INSMF_FORNECEDOR = fornecedores()

    consulta = fetchallmap("""select distinct
                                a.codfor,
                                rtrim(c.dcto01) insmf_empresa,
                                c.razao_social empresa,
                                d.desnom admin,
                                rtrim(d.dcto01) insmf_admin,
                                e.descrc,
                                a.nroRegistro,
                                a.dataRegistro,
                                d.codnom
                            from
                                mat.MXT61600 a
                            join mat.MXT60100 b on
                                a.codfor = b.codfor
                            JOIN mat.MXT61400 c on
                                c.codnom = b.codnom
                            join mat.MXT61400 d on
                                d.codnom = a.codnom
                            join mat.MXT60500 e on
                                a.carsoc = e.codite
                            where
                                e.codsis = 001
                                and e.codtab = 009""")
    
    insert = cur_fdb.prep("""insert into desfor_socios (codif, codif_socio, dt_juntacomer, numregistro, cargosocio, cargo_ant) values (?,?,?,?,?,?)""")

    for row in tqdm(consulta, desc="Vinculando Sócios"):
        try:
            codif_empresa = INSMF_FORNECEDOR.get(row['insmf_empresa'], None)
            codif_socio = INSMF_FORNECEDOR.get(row['insmf_admin'], None)
            cur_fdb.execute(insert, (codif_empresa, codif_socio, row['dataRegistro'], row['nroRegistro'], 1, row['descrc']))
        except:
            continue
    commit()